package actors

import "time"

type ActorRef interface {
	Send(interface{})
	SendFrom(interface{}, ActorRef)
	Ask(interface{}) interface{}
	AskWithTimeout(interface{}, time.Duration) interface{}
	Stop()
}

type LocalActorRef struct {
	actorCell *actorCell
}

func (lar *LocalActorRef) Send(message interface{}) {
	lar.SendFrom(message, nil)
}

func (lar *LocalActorRef) SendFrom(message interface{}, sender ActorRef) {
	lar.actorCell.messages <- messageEnvelope{
		sender:  sender,
		message: message,
	}
}

func (lar *LocalActorRef) Ask(message interface{}) interface{} {
	return lar.AskWithTimeout(message, 3*time.Second)
}

func (lar *LocalActorRef) AskWithTimeout(
	message interface{},
	timeout time.Duration,
) interface{} {
	receiver := make(chan interface{})
	defer close(receiver)

	askActor := SpawnActor(&ChannelActor{receiver})
	defer askActor.Stop()

	lar.SendFrom(message, askActor)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case response := <-receiver:
		return response
	case <-timer.C:
		panic("Ask timed out")
	}
}

func (lar *LocalActorRef) Stop() {
	lar.actorCell.systemMessages <- struct{}{}
}
