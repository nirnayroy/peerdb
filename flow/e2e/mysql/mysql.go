package e2e_postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/connectors/mysql"
	"github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type PeerFlowE2ETestSuiteMySQL struct {
	t *testing.T

	conn   *connmysql.MySqlConnector
	suffix string
}

func (s PeerFlowE2ETestSuiteMySQL) T() *testing.T {
	return s.t
}

func (s PeerFlowE2ETestSuiteMySQL) Connector() *connmysql.MySqlConnector {
	return s.conn
}

func (s PeerFlowE2ETestSuiteMySQL) DestinationConnector() connectors.Connector {
	return s.conn
}

func (s PeerFlowE2ETestSuiteMySQL) Suffix() string {
	return s.suffix
}

func (s PeerFlowE2ETestSuiteMySQL) Peer() *protos.Peer {
	return e2e.GeneratePostgresPeer(s.t)
}

func (s PeerFlowE2ETestSuiteMySQL) DestinationTable(table string) string {
	return e2e.AttachSchema(s, table)
}

func (s PeerFlowE2ETestSuiteMySQL) GetRows(table string, cols string) (*model.QRecordBatch, error) {
	s.t.Helper()
	panic("TODO")
}

func SetupSuite(t *testing.T) PeerFlowE2ETestSuiteMySQL {
	t.Helper()

	suffix := "pg_" + strings.ToLower(shared.RandomString(8))
	conn, err := e2e.SetupPostgres(t, suffix)
	require.NoError(t, err, "failed to setup postgres")

	return PeerFlowE2ETestSuiteMySQL{
		t:      t,
		conn:   conn,
		suffix: suffix,
	}
}

func (s PeerFlowE2ETestSuiteMySQL) Teardown() {
	// TODO for mysql
}
