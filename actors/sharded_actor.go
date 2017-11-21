package actors

type ActorConstructor func(string) Actor
type GetShardFromMessage func(interface{}) int
type GetActorIDFromMessage func(interface{}) string

type shardedActor struct {
	actorFactory          ActorConstructor
	shardCount            int
	getActorIDFromMessage GetActorIDFromMessage
	getShardFromMessage   GetShardFromMessage
	shards                []ActorRef
}

type shardEnvelope struct {
	actorID string
	message interface{}
}

func MakeShardedActor(
	actorConstructor ActorConstructor,
	shardCount int,
	getActorIDFromMessage GetActorIDFromMessage,
	getShardFromMessage GetShardFromMessage,
) Actor {
	return &shardedActor{
		actorFactory:          actorConstructor,
		shardCount:            shardCount,
		getActorIDFromMessage: getActorIDFromMessage,
		getShardFromMessage:   getShardFromMessage,
		shards:                make([]ActorRef, shardCount),
	}
}

func (sa *shardedActor) OnStart(context ActorContext) {
}

func (sa *shardedActor) OnStop(context ActorContext) {
}

func (sa *shardedActor) Receive(context ActorContext) {
	shardID := sa.getShardFromMessage(context.Message())
	shard := sa.getShard(shardID)
	actorID := sa.getActorIDFromMessage(context.Message())
	envelope := shardEnvelope{
		actorID: actorID,
		message: context.Message(),
	}
	context.Forward(envelope, shard)
}

func (sa *shardedActor) receiveUserMessage(context ActorContext) {

}

func (sa *shardedActor) getShard(shardID int) ActorRef {
	ref := sa.shards[shardID]
	if ref == nil {
		return sa.spawnShard(shardID)
	}
	return ref
}

func (sa *shardedActor) spawnShard(shardID int) ActorRef {
	actor := newActorShard(shardID, sa.actorFactory)
	ref := SpawnActor(actor)
	sa.shards[shardID] = ref
	return ref
}

type actorShard struct {
	shardID      int
	actorFactory ActorConstructor
	actors       map[string]ActorRef
}

func newActorShard(shardID int, actorFactory ActorConstructor) Actor {
	return &actorShard{
		shardID:      shardID,
		actorFactory: actorFactory,
		actors:       make(map[string]ActorRef),
	}
}

func (as *actorShard) OnStart(context ActorContext) {
}

func (as *actorShard) OnStop(context ActorContext) {
}

func (as *actorShard) Receive(context ActorContext) {
	message := context.Message().(shardEnvelope)
	ref := as.getActor(message.actorID)
	context.Forward(message.message, ref)
}

func (as *actorShard) getActor(actorID string) ActorRef {
	foundRef, found := as.actors[actorID]
	if found {
		return foundRef
	}
	return as.spawnActor(actorID)
}

func (as *actorShard) spawnActor(actorID string) ActorRef {
	actor := as.actorFactory(actorID)
	ref := SpawnActor(actor)
	as.actors[actorID] = ref
	return ref
}
