package actors_test

import (
	"errors"

	"github.com/golang/mock/gomock"
	. "github.com/kphelps/actors/actors"
	"github.com/kphelps/actors/mocks/actors"
	"github.com/kphelps/streams/streams"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ReadSideActor", func() {
	var handler *actors_mocks.MockReadSideHandler
	var sequenceTracker SequenceTracker
	var actor Actor
	var context *actors_mocks.MockActorContext
	var receiver ActorRef
	var receiveChannel chan interface{}

	makeActor := func(
		sequenceTracker SequenceTracker,
	) {
		actor = NewReadSideActor(
			cassandraSession,
			handler,
			sequenceTracker,
			0,
		)
	}

	expectSequenceStart := func(startID uint64) {
		source := streams.NewSource(func() PersistentEvent {
			return PersistentEvent{}
		})
		handler.EXPECT().EventSource(gomock.Eq(startID)).Return(source)
	}

	BeforeEach(func() {
		receiveChannel = make(chan interface{})
		receiver = SpawnActor(&ChannelActor{receiveChannel})
		handler = actors_mocks.NewMockReadSideHandler(mockCtrl)
		handler.EXPECT().OffsetName().Return("offset").AnyTimes()
		sequenceTracker = NewSequenceTracker(cassandraSession)
		context = actors_mocks.NewMockActorContext(mockCtrl)
		context.EXPECT().Self().Return(receiver).AnyTimes()
		makeActor(sequenceTracker)
	})

	Describe("OnStart", func() {
		Context("Getting current sequence fails", func() {
			It("Panics", func() {
				st := actors_mocks.NewMockSequenceTracker(mockCtrl)
				st.EXPECT().GetSequenceID(gomock.Any()).Return(uint64(0), errors.New("err"))
				makeActor(st)
				Expect(func() { actor.OnStart(context) }).To(Panic())
			})
		})

		Context("First start", func() {
			It("Starts", func() {
				expectSequenceStart(0)
				actor.OnStart(context)
			})
		})

		Context("Next start", func() {
			It("Starts", func() {
				err := sequenceTracker.UpdateSequence("offset", 10).Execute(cassandraSession)
				Expect(err).NotTo(HaveOccurred())
				expectSequenceStart(10)
				actor.OnStart(context)
				Eventually(receiveChannel).Should(Receive())
			})
		})

	})

	Describe("OnStop", func() {
		Context("Never started", func() {
			It("doesn't panic", func() {
				actor.OnStop(context)
			})
		})

		Context("Started", func() {
			It("stops", func() {
				expectSequenceStart(0)
				actor.OnStart(context)
				actor.OnStop(context)
			})
		})
	})

	Describe("Receive", func() {
		It("Handles the event", func() {
			event := PersistentEvent{}
			context.EXPECT().Message().Return(event)
			handler.EXPECT().ReadEvent(gomock.Eq(event)).Return(NewLazyQueryBatch(), nil)
			actor.Receive(context)
			id, err := sequenceTracker.GetSequenceID("offset")
			Expect(err).NotTo(HaveOccurred())
			Expect(id).To(Equal(uint64(1)))
		})

		Context("Handle fails", func() {
			It("Retries", func() {
				event := PersistentEvent{}
				context.EXPECT().Message().Return(event)
				gomock.InOrder(
					handler.EXPECT().ReadEvent(gomock.Eq(event)).
						Return(nil, errors.New("err")).
						MaxTimes(3).
						MinTimes(3),
					handler.EXPECT().ReadEvent(gomock.Eq(event)).
						Return(NewLazyQueryBatch(), nil),
				)
				actor.Receive(context)
				id, err := sequenceTracker.GetSequenceID("offset")
				Expect(err).NotTo(HaveOccurred())
				Expect(id).To(Equal(uint64(1)))
			})
		})
	})
})
