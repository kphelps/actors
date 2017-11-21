package actors

import (
	"errors"
	"sync"

	"github.com/golang/protobuf/proto"
)

var pp PersistenceProvider

func init() {
	pp = cassandraPersistenceProvider()
	err := pp.Initialize()
	if err != nil {
		panic(err)
	}
}

type PersistentEvent struct {
	SequenceID uint64
	Event      proto.Message
}

type PersistenceProvider interface {
	Initialize() error
	GetEvent(actorID string, sequenceID uint64) (PersistentEvent, error)
	GetEvents(actorID string, sequenceID uint64) ([]PersistentEvent, error)
	PersistEvent(actorID string, sequenceID uint64, event proto.Message) error
	MaxSequenceID(actorID string) (uint64, error)
}

type InMemoryPersistenceProvider struct {
	sync.Mutex
	events map[string][]proto.Message
}

func NewPersistenceProvider() PersistenceProvider {
	return &InMemoryPersistenceProvider{
		events: make(map[string][]proto.Message),
	}
}

func GetPersistenceProvider() PersistenceProvider {
	return pp
}

func (i *InMemoryPersistenceProvider) Initialize() error {
	return nil
}

func (imp *InMemoryPersistenceProvider) GetEvents(
	actorID string,
	sequenceID uint64,
) ([]PersistentEvent, error) {
	imp.Lock()
	defer imp.Unlock()

	out := make([]PersistentEvent, uint64(len(imp.events[actorID]))-sequenceID)
	for i := uint64(0); i < uint64(len(imp.events[actorID]))-sequenceID; i++ {
		event, err := imp.GetEvent(actorID, sequenceID+i)
		if err != nil {
			return []PersistentEvent{}, nil
		}
		out[i] = event
	}
	return out, nil
}

func (i *InMemoryPersistenceProvider) GetEvent(
	actorID string,
	sequenceID uint64,
) (PersistentEvent, error) {
	actorEvents, found := i.events[actorID]
	if !found {
		return PersistentEvent{}, errors.New("not found")
	}

	if sequenceID > uint64(len(actorEvents)) {
		return PersistentEvent{}, errors.New("not found")
	}

	event := actorEvents[sequenceID]
	return PersistentEvent{
		SequenceID: sequenceID,
		Event:      event,
	}, nil
}

func (i *InMemoryPersistenceProvider) PersistEvent(
	actorID string,
	sequenceID uint64,
	event proto.Message,
) error {
	i.Lock()
	defer i.Unlock()

	actorEvents, found := i.events[actorID]
	if !found {
		i.events[actorID] = []proto.Message{event}
	} else {
		if sequenceID != uint64(len(actorEvents)) {
			return errors.New("Invalid sequenceID")
		}
		i.events[actorID] = append(actorEvents, event)
	}
	return nil
}

func (i *InMemoryPersistenceProvider) MaxSequenceID(
	actorID string,
) (uint64, error) {
	events, found := i.events[actorID]
	if !found {
		return uint64(0), nil
	}
	return uint64(len(events)), nil
}
