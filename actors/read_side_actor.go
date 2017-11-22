package actors

import (
	"time"

	"github.com/gocql/gocql"
	"github.com/kphelps/streams/streams"
)

type readSideActor struct {
	cassandra            *gocql.Session
	handler              ReadSideHandler
	stream               streams.RunnableStream
	currentSequenceID    uint64
	sequenceTracker      SequenceTracker
	failureSleepDuration time.Duration
}

func NewReadSideActor(
	cassandra *gocql.Session,
	handler ReadSideHandler,
	sequenceTracker SequenceTracker,
	failureSleepDuration time.Duration,
) Actor {
	return &readSideActor{
		cassandra:            cassandra,
		handler:              handler,
		sequenceTracker:      sequenceTracker,
		failureSleepDuration: failureSleepDuration,
	}
}

type ReadSideHandler interface {
	EventSource(startSequenceID uint64) streams.Source
	OffsetName() string
	ReadEvent(event PersistentEvent) (BatchableQuery, error)
}

func (rsa *readSideActor) OnStart(context ActorContext) {
	offset, err := rsa.getCurrentSequenceID()
	if err != nil {
		panic("TODO")
	}
	source := rsa.handler.EventSource(offset)
	sink := streams.NewSink(func(event PersistentEvent) {
		context.Self().Send(event)
	})
	rsa.stream = source.AttachSink(sink)
	rsa.stream.Open()
}

func (rsa *readSideActor) OnStop(context ActorContext) {
	if rsa.stream != nil {
		rsa.stream.Close()
	}
}

func (rsa *readSideActor) Receive(context ActorContext) {
	switch event := context.Message().(type) {
	case PersistentEvent:
		for {
			err := rsa.handle(event)
			if err == nil {
				break
			}
			time.Sleep(rsa.failureSleepDuration)
		}
	}
}

func (rsa *readSideActor) handle(actorEvent PersistentEvent) error {
	q, err := rsa.handler.ReadEvent(actorEvent)
	if err != nil {
		return err
	}

	q = q.Merge(rsa.updateSequenceID(actorEvent.SequenceID + 1))
	return q.Execute(rsa.cassandra)
}

func (rsa *readSideActor) SequenceName() string {
	return rsa.handler.OffsetName()
}

func (rsa *readSideActor) getCurrentSequenceID() (uint64, error) {
	return rsa.sequenceTracker.GetSequenceID(rsa.SequenceName())
}

func (rsa *readSideActor) updateSequenceID(sequenceID uint64) BatchableQuery {
	return rsa.sequenceTracker.UpdateSequence(rsa.SequenceName(), sequenceID)
}
