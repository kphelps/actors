package actors_test

import (
	"sync/atomic"

	. "github.com/kphelps/actors/actors"
	"github.com/kphelps/actors/mocks/actors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type testShardMessage struct {
	actorID string
	shard   int
}

var _ = Describe("ShardedActor", func() {

	var actor Actor
	var ref ActorRef
	var context *actors_mocks.MockActorContext
	var receiveChan chan interface{}
	var constructedCount uint64

	BeforeEach(func() {
		context = actors_mocks.NewMockActorContext(mockCtrl)
		receiveChan = make(chan interface{})
		constructedCount = 0
		actor = MakeShardedActor(
			func(actorID string) Actor {
				atomic.AddUint64(&constructedCount, 1)
				return &ChannelActor{receiveChan}
			},
			10,
			func(message interface{}) string {
				return message.(testShardMessage).actorID
			},
			func(message interface{}) int {
				return message.(testShardMessage).shard
			},
		)
		ref = SpawnActor(actor)
	})

	AfterEach(func() {
		ref.Stop()
	})

	Describe("OnStart", func() {
		It("doesn't panic", func() {
			actor.OnStart(context)
		})
	})

	Describe("OnStop", func() {
		It("doesn't panic", func() {
			actor.OnStop(context)
		})
	})

	Describe("Receive", func() {
		It("Forwards to a shard", func() {
			message := testShardMessage{"hello", 1}
			ref.Send(message)
			Eventually(receiveChan).Should(Receive(Equal(message)))
			Expect(constructedCount).To(Equal(uint64(1)))
		})

		It("Forwards to the same actor repeatedly", func() {
			message := testShardMessage{"hello", 1}
			ref.Send(message)
			ref.Send(message)
			ref.Send(message)
			Eventually(receiveChan).Should(Receive(Equal(message)))
			Eventually(receiveChan).Should(Receive(Equal(message)))
			Eventually(receiveChan).Should(Receive(Equal(message)))
			Expect(constructedCount).To(Equal(uint64(1)))
		})

		It("Can have multiple shards", func() {
			message1 := testShardMessage{"hello", 1}
			ref.Send(message1)
			Eventually(receiveChan).Should(Receive(Equal(message1)))

			message2 := testShardMessage{"hello", 2}
			ref.Send(message2)
			Eventually(receiveChan).Should(Receive(Equal(message2)))

			Expect(constructedCount).To(Equal(uint64(2)))
		})
	})
})
