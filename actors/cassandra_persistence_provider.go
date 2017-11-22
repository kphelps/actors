package actors

import (
	"reflect"
	"time"

	"github.com/gocql/gocql"
	"github.com/golang/protobuf/proto"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
)

type CassandraPersistenceProvider struct {
	session        *gocql.Session
	keyspace       string
	partitionCount uint64
}

func ConfigureCassandraPersistenceProvider(
	keyspace string,
) error {
	provider := cassandraPersistenceProvider(keyspace)
	return initPersistenceProvider(provider)
}

func cassandraPersistenceProvider(keyspace string) PersistenceProvider {
	return &CassandraPersistenceProvider{
		keyspace:       keyspace,
		partitionCount: uint64(10),
	}
}

func (c *CassandraPersistenceProvider) eventSerializer(
	event proto.Message,
) ([]byte, error) {
	return proto.Marshal(event)
}

func (c *CassandraPersistenceProvider) eventDeserializer(
	typeName string,
	data []byte,
) (proto.Message, error) {
	messageType := proto.MessageType(typeName).Elem()
	message := reflect.New(messageType).Interface().(proto.Message)
	err := proto.Unmarshal(data, message)
	return message, err
}

func (c *CassandraPersistenceProvider) Initialize() error {
	err := c.createKeyspace()
	if err != nil {
		return err
	}

	err = c.initializeSession(60 * time.Second)
	if err != nil {
		return err
	}

	err = c.createActorEventsTable()
	if err != nil {
		return err
	}

	err = c.createSequenceIDTable()
	if err != nil {
		return err
	}

	return c.initializeSession(3 * time.Second)
}

func (c *CassandraPersistenceProvider) initializeSession(timeout time.Duration) error {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = c.keyspace
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = timeout
	session, err := cluster.CreateSession()
	c.session = session
	return err
}

func (c *CassandraPersistenceProvider) connectWithoutKeyspace() (*gocql.Session, error) {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Timeout = 60 * time.Second
	return cluster.CreateSession()
}

func (c *CassandraPersistenceProvider) createKeyspace() error {
	session, err := c.connectWithoutKeyspace()
	if err != nil {
		return err
	}

	err = session.Query(
		`CREATE KEYSPACE IF NOT EXISTS ` + c.keyspace + `
		WITH REPLICATION = {
			'class': 'SimpleStrategy',
			'replication_factor': 1
		}`,
	).Exec()
	return err
}

func (c *CassandraPersistenceProvider) createActorEventsTable() error {
	return c.session.Query(
		`CREATE TABLE IF NOT EXISTS actor_events (
			actor_id text,
			partition_id bigint,
			sequence_id bigint,
			timestamp timeuuid,
			event blob,
			event_type text,
			PRIMARY KEY ((actor_id, partition_id), sequence_id)
		)`,
	).Exec()
}

func (c *CassandraPersistenceProvider) createSequenceIDTable() error {
	return c.session.Query(
		`CREATE TABLE IF NOT EXISTS sequence_ids (
			name text,
			sequence_id bigint,
			PRIMARY KEY (name)
		)`,
	).Exec()
}

func (c *CassandraPersistenceProvider) partitionIDFromSequenceID(
	sequenceID uint64,
) uint64 {
	return sequenceID % c.partitionCount
}

func (c *CassandraPersistenceProvider) MaxSequenceID(
	actorID string,
) (uint64, error) {
	maxSequenceID := uint64(0)
	for i := uint64(0); i < c.partitionCount; i++ {
		value, err := c.PartitionMaxSequenceID(actorID, i)
		if err == nil {
			if value > maxSequenceID {
				maxSequenceID = value
			}
		} else if err.Error() == "not found" {
			continue
		} else {
			return 0, err
		}
	}
	return maxSequenceID, nil
}

func (c *CassandraPersistenceProvider) PartitionMaxSequenceID(
	actorID string,
	partitionID uint64,
) (uint64, error) {
	stmt, names := qb.Select("actor_events").
		Columns("sequence_id").
		Where(
			qb.Eq("actor_id"),
			qb.Eq("partition_id"),
		).
		OrderBy("sequence_id", qb.DESC).
		Limit(1).
		ToCql()
	q := gocqlx.Query(c.session.Query(stmt), names).BindMap(qb.M{
		"actor_id":     actorID,
		"partition_id": partitionID,
	})
	var sequenceID uint64
	err := gocqlx.Get(&sequenceID, q.Query)
	return sequenceID, err
}

type eventEnvelope struct {
	Event     []byte
	EventType string
}

func (c *CassandraPersistenceProvider) GetEvent(
	actorID string,
	sequenceID uint64,
) (PersistentEvent, error) {
	stmt, names := qb.Select("actor_events").
		Columns("event", "event_type").
		Where(
			qb.Eq("actor_id"),
			qb.Eq("partition_id"),
			qb.Eq("sequence_id"),
		).
		ToCql()
	q := gocqlx.Query(c.session.Query(stmt), names).BindMap(qb.M{
		"actor_id":     actorID,
		"partition_id": c.partitionIDFromSequenceID(sequenceID),
		"sequence_id":  sequenceID,
	})
	var envelope eventEnvelope
	err := gocqlx.Get(&envelope, q.Query)
	if err != nil {
		return PersistentEvent{}, err
	}
	event, err := c.eventDeserializer(envelope.EventType, envelope.Event)
	return PersistentEvent{
		SequenceID: sequenceID,
		Event:      event,
	}, err
}

func (c *CassandraPersistenceProvider) GetEventsInclusive(
	actorID string,
	minSequenceID uint64,
	maxSequenceID uint64,
) ([]PersistentEvent, error) {
	length := maxSequenceID - minSequenceID + 1
	if length < 0 {
		length = 0
	}

	events := make([]PersistentEvent, length)
	for i := minSequenceID; i <= maxSequenceID; i++ {
		event, err := c.GetEvent(actorID, i)
		if err != nil {
			return nil, err
		}
		events[i-minSequenceID] = event
	}
	return events, nil
}

func (c *CassandraPersistenceProvider) GetEvents(
	actorID string,
	sequenceID uint64,
) ([]PersistentEvent, error) {
	maxSequenceID, err := c.MaxSequenceID(actorID)
	if err != nil {
		return nil, err
	}
	return c.GetEventsInclusive(actorID, sequenceID, maxSequenceID)
}

func (c *CassandraPersistenceProvider) PersistEvent(
	actorID string,
	sequenceID uint64,
	event proto.Message,
) error {
	stmt, names := qb.Insert("actor_events").
		Columns(
			"actor_id",
			"partition_id",
			"sequence_id",
			"timestamp",
			"event",
			"event_type",
		).
		ToCql()
	serializedEvent, err := c.eventSerializer(event)
	if err != nil {
		return err
	}

	q := gocqlx.Query(c.session.Query(stmt), names).BindMap(qb.M{
		"actor_id":     actorID,
		"partition_id": c.partitionIDFromSequenceID(sequenceID),
		"sequence_id":  sequenceID,
		"timestamp":    gocql.TimeUUID(),
		"event":        serializedEvent,
		"event_type":   proto.MessageName(event),
	})
	return q.ExecRelease()
}
