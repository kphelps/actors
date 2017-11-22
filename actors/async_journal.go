package actors

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/golang/protobuf/proto"
	"github.com/kphelps/streams/streams"
)

type shardedAsyncJournal struct {
	shardCount   int
	writeSide    Actor
	writeSideRef ActorRef
	readSide     Actor
	readSideRef  ActorRef
}

func NewAsyncJournal(
	cassandra *gocql.Session,
	sequenceTracker SequenceTracker,
	impl AsyncJournalImpl,
	shardCount int,
) Actor {
	return &shardedAsyncJournal{
		shardCount: shardCount,
		writeSide:  makeWriteSide(impl, shardCount),
		readSide:   makeReadSide(cassandra, sequenceTracker, impl, shardCount),
	}
}

func makeWriteSide(
	impl AsyncJournalImpl,
	shardCount int,
) Actor {
	return MakeShardedActor(
		func(actorID string) Actor {
			return NewPersistentActor(
				NewAsyncJournalWriteSide(impl, actorID),
			)
		},
		shardCount,
		impl.GetActorIDFromMessage,
		impl.GetShardFromMessage,
	)
}

func makeReadSide(
	cassandra *gocql.Session,
	sequenceTracker SequenceTracker,
	impl AsyncJournalImpl,
	shardCount int,
) Actor {
	return MakeShardedActor(
		func(actorID string) Actor {
			return NewReadSideActor(
				cassandra,
				NewAsyncJournalReadSide(impl, actorID),
				sequenceTracker,
				time.Second,
			)
		},
		shardCount,
		impl.GetActorIDFromMessage,
		impl.GetShardFromMessage,
	)
}

func (saj *shardedAsyncJournal) OnStart(context ActorContext) {
	saj.writeSideRef = SpawnActor(saj.writeSide)
	saj.readSideRef = SpawnActor(saj.readSide)
}

func (saj *shardedAsyncJournal) OnStop(ActorContext) {
}

func (saj *shardedAsyncJournal) Receive(context ActorContext) {
	context.Forward(context.Message(), saj.writeSideRef)
	// TODO: read sides should always be running
	saj.readSideRef.Send(context.Message())
}

type asyncJournalWriteSide struct {
	AsyncJournalImpl
	actorID string
}

type asyncJournalReadSide struct {
	AsyncJournalImpl
	actorID string
}

type AsyncJournalImpl interface {
	Name() string
	GetActorIDFromMessage(message interface{}) string
	GetShardFromMessage(message interface{}) int

	Receive(context PersistentContext)
	HandleEvent(event proto.Message)
	HandleRecover(event proto.Message)

	ReadEvent(event PersistentEvent) (BatchableQuery, error)
}

func NewAsyncJournalReadSide(
	impl AsyncJournalImpl,
	actorID string,
) ReadSideHandler {
	return &asyncJournalReadSide{
		AsyncJournalImpl: impl,
		actorID:          actorID,
	}
}

func (ajrs *asyncJournalReadSide) EventSource(sequenceID uint64) streams.Source {
	return NewActorEventSource(ajrs.actorID, sequenceID)
}

func (ajrs *asyncJournalReadSide) OffsetName() string {
	return fmt.Sprintf("%s:%s", ajrs.Name(), ajrs.actorID)
}

func NewAsyncJournalWriteSide(
	impl AsyncJournalImpl,
	actorID string,
) PersistentActor {
	return &asyncJournalWriteSide{
		AsyncJournalImpl: impl,
		actorID:          actorID,
	}
}

func (ajws *asyncJournalWriteSide) PersistenceID() string {
	return ajws.actorID
}
