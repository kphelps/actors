package actors

type ChannelActor struct {
	Output chan interface{}
}

func (ca *ChannelActor) OnStart(context ActorContext) {

}

func (ca *ChannelActor) OnStop(context ActorContext) {

}

func (ca *ChannelActor) Receive(context ActorContext) {
	ca.Output <- context.Message()
}
