package actors_test

import (
	"sync"

	. "github.com/kphelps/actors/actors"
	"github.com/kphelps/actors/mocks/actors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Actors", func() {

	var actors []ActorRef
	var lock sync.Mutex
	var calls int

	startActor := func(handler func(context ActorContext)) ActorRef {
		ref := SpawnActor(NewFunctionActor(func(context ActorContext) {
			defer GinkgoRecover()
			lock.Lock()
			defer lock.Unlock()
			handler(context)
			calls++
		}))
		actors = append(actors, ref)
		return ref
	}

	getCalls := func() int {
		lock.Lock()
		defer lock.Unlock()
		return calls
	}

	BeforeEach(func() {
		calls = 0
		actors = make([]ActorRef, 0)
	})

	AfterEach(func() {
		for _, actor := range actors {
			actor.Stop()
		}
	})

	Context("Send", func() {
		It("Is received", func() {
			var received int
			ref := startActor(func(context ActorContext) {
				received = context.Message().(int)
			})
			ref.Send(5)
			Eventually(getCalls).Should(Equal(1))
			Expect(received).To(Equal(5))
		})
	})

	Context("SendFrom", func() {
		It("Sets the sender", func() {
			sender := actors_mocks.NewMockActorRef(mockCtrl)
			ref := startActor(func(context ActorContext) {
				Expect(context.Sender()).NotTo(BeNil())
			})
			ref.SendFrom(nil, sender)
			Eventually(getCalls).Should(Equal(1))
		})
	})

	Context("Ask", func() {
		It("Receives a response", func() {
			ref := startActor(func(context ActorContext) {
				context.Reply(context.Message())
			})
			response := ref.Ask(5)
			Expect(response.(int)).To(Equal(5))
		})
	})

	Context("AskWithTimeout", func() {
		It("Panics after timeout", func() {
			ref := startActor(func(ActorContext) {})
			Expect(func() { ref.AskWithTimeout(5, 0) }).To(Panic())
		})
	})
})
