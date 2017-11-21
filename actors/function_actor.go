package actors

type functionActor struct {
	receiver func(context ActorContext)
}

func NewFunctionActor(receiver func(context ActorContext)) Actor {
	return &functionActor{
		receiver: receiver,
	}
}

func (fa *functionActor) OnStart(ActorContext) {

}

func (fa *functionActor) OnStop(ActorContext) {

}

func (fa *functionActor) Receive(context ActorContext) {
	fa.receiver(context)
}
