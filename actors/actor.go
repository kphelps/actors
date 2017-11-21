package actors

type Actor interface {
	OnStart(ActorContext)
	OnStop(ActorContext)
	Receive(ActorContext)
}

func SpawnActor(actor Actor) ActorRef {
	cell := actorCell{
		actor:          actor,
		state:          actorStopped,
		messages:       make(chan messageEnvelope, 10),
		systemMessages: make(chan interface{}),
	}
	cell.start()
	return &LocalActorRef{
		actorCell: &cell,
	}
}
