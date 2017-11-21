package actors_test

import (
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"

	"testing"
)

var mockCtrl *gomock.Controller
var cassandraSession *gocql.Session
var keyspaceMetadata *gocql.KeyspaceMetadata

func Session() *gocql.Session {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Timeout = 60 * time.Second
	cluster.Keyspace = "actors_test"
	session, err := cluster.CreateSession()
	if err != nil {
		Expect(err).NotTo(HaveOccurred())
	}
	return session
}

func UpdateSchema() error {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Timeout = 60 * time.Second
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	err = session.Query(`CREATE KEYSPACE IF NOT EXISTS actors_test WITH REPLICATION = {
		'class': 'SimpleStrategy',
		'replication_factor': 1
	}`).Exec()
	return err
}

func getKeyspaceMetadata(session *gocql.Session) (*gocql.KeyspaceMetadata, error) {
	return session.KeyspaceMetadata("actors_test")
}

// Cassandra's `TRUNCATE` is very slow for some reason. Instead we just scan
// every table, pull out the partition keys, and do a batch delete for every
// row.
func TruncateTables(session *gocql.Session) (int, error) {
	count := 0
	for table_name, table_metadata := range keyspaceMetadata.Tables {
		if table_name == "gocqlx_migrate" {
			continue
		}
		columns := make([]qb.Cmp, 0)
		for _, columnMetadata := range table_metadata.PartitionKey {
			columns = append(columns, qb.Eq(columnMetadata.Name))
		}
		columnNames := make([]string, 0)
		for _, columnMetadata := range table_metadata.PartitionKey {
			columnNames = append(columnNames, columnMetadata.Name)
		}
		columnStr := strings.Join(columnNames, ",")
		rowIter := session.Query("SELECT DISTINCT " + columnStr + " FROM " + table_name).Iter()
		for {
			rows, err := rowIter.SliceMap()
			if err != nil {
				return 0, err
			}
			if len(rows) == 0 {
				break
			}
			stmt, names := qb.Delete(table_name).Where(columns...).ToCql()

			for _, row := range rows {
				values := make([]interface{}, 0)
				for _, columnMetadata := range table_metadata.PartitionKey {
					values = append(values, row[columnMetadata.Name])
				}
				err := gocqlx.Query(session.Query(stmt, values...), names).ExecRelease()
				if err != nil {
					return 0, err
				}
				count += 1
			}
		}
	}
	return count, nil
}

var _ = BeforeSuite(func() {
	err := UpdateSchema()
	Expect(err).NotTo(HaveOccurred())

	cassandraSession = Session()

	keyspaceMetadata, err = getKeyspaceMetadata(cassandraSession)
	Expect(err).NotTo(HaveOccurred())
})

var _ = BeforeEach(func() {
	mockCtrl = gomock.NewController(GinkgoT())
	_, err := TruncateTables(cassandraSession)
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterEach(func() {
	mockCtrl.Finish()
	mockCtrl = nil
	_, err := TruncateTables(cassandraSession)
	Expect(err).NotTo(HaveOccurred())
})

func TestActors(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Actors Suite")
}
