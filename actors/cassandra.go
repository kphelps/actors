package actors

import (
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

type CassandraConfig struct {
	Host     string
	Keyspace string
}

func CassandraConnect(config CassandraConfig) (*gocql.Session, error) {
	cluster := gocql.NewCluster(config.Host)
	cluster.Timeout = 3000 * time.Millisecond
	cluster.Keyspace = config.Keyspace
	cluster.Consistency = gocql.Quorum
	return cluster.CreateSession()
}

type BatchableQuery interface {
	Execute(session *gocql.Session) error
	AddToBatch(batch *gocql.Batch)
	Merge(q BatchableQuery) BatchableQuery
	QueryCount() int
	String() string
}

type LazyQuery struct {
	Statement string
	Values    []interface{}
}

func QueryFromMap(statement string, names []string, inValues map[string]interface{}) BatchableQuery {
	values := make([]interface{}, len(names))
	for i, name := range names {
		v, found := inValues[name]
		if !found {
			panic("Could not find: " + name)
		}
		values[i] = v
	}
	return &LazyQuery{
		Statement: statement,
		Values:    values,
	}
}

func (lq *LazyQuery) Execute(session *gocql.Session) error {
	return session.Query(lq.Statement, lq.Values...).Exec()
}

func (lq *LazyQuery) AddToBatch(batch *gocql.Batch) {
	batch.Query(lq.Statement, lq.Values...)
}

func (lq *LazyQuery) Merge(q BatchableQuery) BatchableQuery {
	return NewLazyQueryBatch(lq, q)
}

func (lq *LazyQuery) QueryCount() int {
	return 1
}

func (lq *LazyQuery) String() string {
	return fmt.Sprintf("%s [%s]", lq.Statement, lq.Values)
}

type LazyQueryBatch struct {
	Queries []BatchableQuery
}

func NewLazyQueryBatch(queries ...BatchableQuery) BatchableQuery {
	return &LazyQueryBatch{
		Queries: queries,
	}
}

func (lqb *LazyQueryBatch) Execute(session *gocql.Session) error {
	batch := session.NewBatch(gocql.LoggedBatch)
	lqb.AddToBatch(batch)
	return session.ExecuteBatch(batch)
}

func (lq *LazyQueryBatch) AddToBatch(batch *gocql.Batch) {
	for _, query := range lq.Queries {
		query.AddToBatch(batch)
	}
}

func (lq *LazyQueryBatch) Merge(q BatchableQuery) BatchableQuery {
	return NewLazyQueryBatch(lq, q)
}

func (lq *LazyQueryBatch) QueryCount() int {
	sum := 0
	for _, query := range lq.Queries {
		sum += query.QueryCount()
	}
	return sum
}

func (lq *LazyQueryBatch) String() string {
	queries := make([]string, len(lq.Queries))
	for i, query := range lq.Queries {
		queries[i] = query.String()
	}
	return strings.Join(queries, ";\n")
}
