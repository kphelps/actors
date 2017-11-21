package actors

type actorCellState int

const (
	actorStopped actorCellState = iota
	actorRunning
)

type messageEnvelope struct {
	sender  ActorRef
	message interface{}
}

type actorCell struct {
	actor          Actor
	state          actorCellState
	messages       chan messageEnvelope
	systemMessages chan interface{}
}

func (ac *actorCell) start() {
	go ac.loop()
}

func (ac *actorCell) loop() {
	context := actorContextImpl{
		self: &LocalActorRef{
			actorCell: ac,
		},
	}
	ac.actor.OnStart(&context)
	defer ac.actor.OnStop(&context)

	for {
		select {
		case message := <-ac.messages:
			context.message = message.message
			context.sender = message.sender
			ac.actor.Receive(&context)
			context.message = nil

		case <-ac.systemMessages:
			break
		}
	}
}
