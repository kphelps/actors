package actors

import (
	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
)

type SequenceTracker interface {
	UpdateSequence(sequenceName string, sequenceID uint64) BatchableQuery
	GetSequenceID(sequenceName string) (uint64, error)
}

type sequenceTrackerImpl struct {
	cassandra *gocql.Session
}

func NewSequenceTracker(cass *gocql.Session) SequenceTracker {
	return &sequenceTrackerImpl{
		cassandra: cass,
	}
}

func (sti *sequenceTrackerImpl) UpdateSequence(
	sequenceName string,
	sequenceID uint64,
) BatchableQuery {
	stmt, names := qb.Update("sequence_ids").
		Where(qb.Eq("name")).
		Set("sequence_id").
		ToCql()
	return QueryFromMap(stmt, names, qb.M{
		"name":        sequenceName,
		"sequence_id": sequenceID,
	})
}

func (sti *sequenceTrackerImpl) GetSequenceID(
	sequenceName string,
) (uint64, error) {
	stmt, names := qb.Select("sequence_ids").
		Columns("sequence_id").
		Where(qb.Eq("name")).
		ToCql()

	q := gocqlx.Query(sti.cassandra.Query(stmt), names).BindMap(qb.M{
		"name": sequenceName,
	})
	var sequenceID uint64
	err := gocqlx.Get(&sequenceID, q.Query)
	if err == gocql.ErrNotFound {
		return 0, nil
	}
	return sequenceID, err
}
