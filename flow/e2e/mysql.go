package e2e

import (
	"context"
	"fmt"
	"testing"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/connectors/mysql"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

type MySqlSource struct {
	*connmysql.MySqlConnector
}

var mysqlConfig = &protos.MySqlConfig{
	Host:        "localhost",
	Port:        3306,
	User:        "root",
	Password:    "maria",
	Database:    "default",
	Setup:       nil,
	Compression: 0,
	DisableTls:  true,
	Flavor:      "mariadb",
}

func SetupMySQL(t *testing.T, suffix string) (*MySqlSource, error) {
	t.Helper()

	connector, err := connmysql.NewMySqlConnector(context.Background(), mysqlConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres connection: %w", err)
	}

	if _, err := connector.Execute(context.Background(), "DROP DATABASE IF EXISTS e2e_test_"+suffix); err != nil {
		connector.Close()
		return nil, err
	}

	if _, err := connector.Execute(context.Background(), "CREATE DATABASE e2e_test_"+suffix); err != nil {
		connector.Close()
		return nil, err
	}

	return &MySqlSource{MySqlConnector: connector}, nil
}

func (s *MySqlSource) Connector() connectors.Connector {
	return s.MySqlConnector
}

func (s *MySqlSource) Teardown(t *testing.T, suffix string) {
	t.Helper()
	if _, err := s.MySqlConnector.Execute(context.Background(), "DROP DATABASE IF EXISTS e2e_test_"+suffix); err != nil {
		t.Log("failed to drop mysql database", err)
		s.MySqlConnector.Close()
	}
}

func (s *MySqlSource) GeneratePeer(t *testing.T) *protos.Peer {
	t.Helper()
	peer := &protos.Peer{
		Name: "catalog",
		Type: protos.DBType_MYSQL,
		Config: &protos.Peer_MysqlConfig{
			MysqlConfig: mysqlConfig,
		},
	}
	CreatePeer(t, peer)
	return peer
}
