package actors

import (
	"time"

	"github.com/kphelps/streams/streams"
)

type EventStream struct {
	Output chan PersistentEvent
	stream streams.RunnableStream
}

func NewActorEventSource(
	actorID string,
	sequenceID uint64,
) streams.Source {
	pp := GetPersistenceProvider()
	return streams.NewSource(func() PersistentEvent {
		for {
			event, err := pp.GetEvent(actorID, sequenceID)
			if err == nil {
				sequenceID++
				return event
			}
			time.Sleep(time.Second)
		}
	})
}

func NewActorEventStream(
	actorID string,
	sequenceID uint64,
) *EventStream {
	source := NewActorEventSource(actorID, sequenceID)
	output := make(chan PersistentEvent)
	sink := streams.NewSink(func(event PersistentEvent) {
		output <- event
	})
	stream := source.AttachSink(sink)
	return &EventStream{
		Output: output,
		stream: stream,
	}
}

func (es *EventStream) Open() {
	es.stream.Open()
}

func (es *EventStream) Close() {
	es.stream.Close()
}
