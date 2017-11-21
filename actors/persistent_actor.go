package actors

import "github.com/golang/protobuf/proto"

type PersistentActor interface {
	PersistenceID() string
	Receive(context PersistentContext)
	HandleEvent(event proto.Message)
	HandleRecover(event proto.Message)
}

type PersistentContext interface {
	ActorContext
	Persist(event proto.Message)
}

type persistentContextImpl struct {
	ActorContext
	id         string
	sequenceID uint64
	pp         PersistenceProvider
	liveEvents []proto.Message
}

func newPersistentContext() persistentContextImpl {
	return persistentContextImpl{
		pp:         GetPersistenceProvider(),
		liveEvents: make([]proto.Message, 0),
	}
}

func (pci *persistentContextImpl) Persist(event proto.Message) {
	err := pci.pp.PersistEvent(pci.id, pci.sequenceID, event)
	if err != nil {
		panic(err)
	}
	pci.sequenceID++
	pci.liveEvents = append(pci.liveEvents, event)
}

func (pci *persistentContextImpl) Events() ([]PersistentEvent, error) {
	return pci.pp.GetEvents(pci.id, 0)
}

type persistentActorCell struct {
	inner             PersistentActor
	persistentContext persistentContextImpl
}

func NewPersistentActor(
	actor PersistentActor,
) Actor {
	return &persistentActorCell{
		inner:             actor,
		persistentContext: newPersistentContext(),
	}
}

func SpawnPersistentActor(
	actor PersistentActor,
) ActorRef {
	cell := NewPersistentActor(actor)
	return SpawnActor(cell)
}

func (pac *persistentActorCell) OnStart(
	context ActorContext,
) {
	pac.persistentContext.ActorContext = context
	pac.persistentContext.id = pac.inner.PersistenceID()
	events, err := pac.persistentContext.Events()
	if err == nil {
		for _, event := range events {
			pac.inner.HandleRecover(event.Event)
			pac.persistentContext.sequenceID = event.SequenceID + 1
		}
	} else if err.Error() == "not found" {
		return
	} else {
		panic(err)
	}
}

func (pac *persistentActorCell) Receive(
	context ActorContext,
) {
	switch context.Message().(type) {
	default:
		pac.inner.Receive(&pac.persistentContext)
		pac.processEvents()
	}
}

func (pac *persistentActorCell) processEvents() {
	for _, event := range pac.persistentContext.liveEvents {
		pac.inner.HandleEvent(event)
	}
	pac.persistentContext.liveEvents = make([]proto.Message, 0)
}

func (pac *persistentActorCell) OnStop(
	context ActorContext,
) {
}
