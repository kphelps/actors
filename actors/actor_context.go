package actors

type ActorContext interface {
	Message() interface{}
	Reply(message interface{})
	Forward(message interface{}, target ActorRef)
	Self() ActorRef
	Sender() ActorRef
}

type actorContextImpl struct {
	self    ActorRef
	sender  ActorRef
	message interface{}
}

func (a *actorContextImpl) Self() ActorRef {
	return a.self
}

func (a *actorContextImpl) Reply(message interface{}) {
	a.Sender().SendFrom(message, a.Self())
}

func (a *actorContextImpl) Forward(
	message interface{},
	target ActorRef,
) {
	target.SendFrom(message, a.Sender())
}

func (a *actorContextImpl) Sender() ActorRef {
	return a.sender
}

func (a *actorContextImpl) Message() interface{} {
	return a.message
}
