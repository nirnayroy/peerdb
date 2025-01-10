// https://airbyte.com/blog/replicating-mysql-a-look-at-the-binlog-and-gtids

package connmysql

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type MySqlConnector struct {
	*metadataStore.PostgresMetadata
	config *protos.MySqlConfig
	conn   *client.Conn
	logger log.Logger
}

func NewMySqlConnector(ctx context.Context, config *protos.MySqlConfig) (*MySqlConnector, error) {
	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		return nil, err
	}
	return &MySqlConnector{
		PostgresMetadata: pgMetadata,
		config:           config,
		logger:           shared.LoggerFromCtx(ctx),
	}, nil
}

func (c *MySqlConnector) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *MySqlConnector) ConnectionActive(context.Context) error {
	if c.conn != nil {
		return c.conn.Ping()
	}
	return nil
}

func (c *MySqlConnector) connect(ctx context.Context) (*client.Conn, error) {
	argF := []client.Option{func(conn *client.Conn) error {
		conn.SetCapability(mysql.CLIENT_COMPRESS)
		if !c.config.DisableTls {
			conn.SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS13})
		}
		return nil
	}}
	conn, err := client.ConnectWithContext(ctx, fmt.Sprintf("%s:%d", c.config.Host, c.config.Port),
		c.config.User, c.config.Password, c.config.Database, time.Minute, argF...)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Execute("SET sql_mode = ANSI"); err != nil {
		return nil, fmt.Errorf("failed to set sql_mode to ANSI: %w", err)
	}
	return conn, nil
}

func (c *MySqlConnector) Execute(ctx context.Context, cmd string, args ...interface{}) (*mysql.Result, error) {
	slog.Info("mymymy", slog.String("query", cmd), slog.Any("when", time.Now()))
	reconnects := 3
	for {
		// TODO need new connection if ctx changes between calls, or make upstream PR
		if c.conn == nil {
			var err error
			c.conn, err = c.connect(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to connect to mysql server: %w", err)
			}
		}

		rs, err := c.conn.Execute(cmd, args...)
		if err != nil {
			if reconnects > 0 && mysql.ErrorEqual(err, mysql.ErrBadConn) {
				reconnects -= 1
				_ = c.conn.Close()
				c.conn = nil
				continue
			}
			return nil, err
		}
		return rs, nil
	}
}

func (c *MySqlConnector) ExecuteSelectStreaming(ctx context.Context, cmd string, result *mysql.Result,
	rowCb client.SelectPerRowCallback,
	resultCb client.SelectPerResultCallback,
	args ...interface{},
) error {
	slog.Info("mymymy stream", slog.String("query", cmd), slog.Any("when", time.Now()))
	reconnects := 3
	for {
		// TODO need new connection if ctx changes between calls, or make upstream PR
		if c.conn == nil {
			var err error
			c.conn, err = c.connect(ctx)
			if err != nil {
				return fmt.Errorf("failed to connect to mysql server: %w", err)
			}
		}

		cmd = strings.ReplaceAll(cmd, "\"", "`") // please don't work

		if len(args) == 0 { // testing this branch being disabled
			if err := c.conn.ExecuteSelectStreaming(cmd, result, rowCb, resultCb); err != nil {
				if reconnects > 0 && mysql.ErrorEqual(err, mysql.ErrBadConn) {
					reconnects -= 1
					_ = c.conn.Close()
					c.conn = nil
					continue
				}
				return err
			}
		} else {
			stmt, err := c.conn.Prepare(cmd)
			if err != nil {
				if reconnects > 0 && mysql.ErrorEqual(err, mysql.ErrBadConn) {
					reconnects -= 1
					c.conn.Close()
					c.conn = nil
					continue
				}
				return err
			}
			if err := stmt.ExecuteSelectStreaming(result, rowCb, resultCb, args...); err != nil {
				if reconnects > 0 && mysql.ErrorEqual(err, mysql.ErrBadConn) {
					reconnects -= 1
					c.conn.Close()
					c.conn = nil
					continue
				}
				return err
			}
		}
	}
}

func (c *MySqlConnector) GetGtidModeOn(ctx context.Context) (bool, error) {
	rr, err := c.Execute(ctx, "select @@global.gtid_mode")
	if err != nil {
		return false, err
	}

	gtid_mode, err := rr.GetString(0, 0)
	if err != nil {
		return false, err
	}

	return gtid_mode == "ON", nil
}

func (c *MySqlConnector) GetMasterPos(ctx context.Context) (mysql.Position, error) {
	showBinlogStatus := "SHOW BINARY LOG STATUS"
	if eq, err := c.conn.CompareServerVersion("8.4.0"); (err == nil) && (eq < 0) {
		showBinlogStatus = "SHOW MASTER STATUS"
	}

	rr, err := c.Execute(ctx, showBinlogStatus)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("failed to SHOW BINARY LOG STATUS: %w", err)
	}

	name, _ := rr.GetString(0, 0)
	pos, _ := rr.GetInt(0, 1)

	return mysql.Position{Name: name, Pos: uint32(pos)}, nil
}

func (c *MySqlConnector) GetMasterGTIDSet(ctx context.Context) (mysql.GTIDSet, error) {
	var query string
	switch c.config.Flavor {
	case mysql.MariaDBFlavor:
		query = "select @@global.gtid_current_pos"
	default:
		query = "select @@global.gtid_executed"
	}
	rr, err := c.Execute(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to select @@global.gtid_executed: %w", err)
	}
	gx, err := rr.GetString(0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to GetString for gtid_executed: %w", err)
	}
	gset, err := mysql.ParseGTIDSet(c.config.Flavor, gx)
	if err != nil {
		return nil, fmt.Errorf("failed to parse GTID from gtid_executed: %w", err)
	}
	return gset, nil
}

func (c *MySqlConnector) GetVersion(ctx context.Context) (string, error) {
	rr, err := c.Execute(ctx, "select @@version")
	if err != nil {
		return "", err
	}
	version, _ := rr.GetString(0, 0)
	c.logger.Info("[mysql] version", slog.String("version", version))
	return version, nil
}

func qkindFromMysql(ty uint8) (qvalue.QValueKind, error) {
	switch ty {
	case mysql.MYSQL_TYPE_DECIMAL:
		return qvalue.QValueKindNumeric, nil
	case mysql.MYSQL_TYPE_TINY:
		return qvalue.QValueKindInt16, nil // TODO qvalue.QValueKindInt8
	case mysql.MYSQL_TYPE_SHORT:
		return qvalue.QValueKindInt16, nil
	case mysql.MYSQL_TYPE_LONG:
		return qvalue.QValueKindInt32, nil
	case mysql.MYSQL_TYPE_FLOAT:
		return qvalue.QValueKindFloat32, nil
	case mysql.MYSQL_TYPE_DOUBLE:
		return qvalue.QValueKindFloat64, nil
	case mysql.MYSQL_TYPE_NULL:
		return qvalue.QValueKindInvalid, nil // TODO qvalue.QValueKindNothing
	case mysql.MYSQL_TYPE_TIMESTAMP:
		return qvalue.QValueKindTimestamp, nil
	case mysql.MYSQL_TYPE_LONGLONG:
		return qvalue.QValueKindInt64, nil
	case mysql.MYSQL_TYPE_INT24:
		return qvalue.QValueKindInt32, nil
	case mysql.MYSQL_TYPE_DATE:
		return qvalue.QValueKindDate, nil
	case mysql.MYSQL_TYPE_TIME:
		return qvalue.QValueKindTime, nil
	case mysql.MYSQL_TYPE_DATETIME:
		return qvalue.QValueKindTimestamp, nil
	case mysql.MYSQL_TYPE_YEAR:
		return qvalue.QValueKindInt16, nil
	case mysql.MYSQL_TYPE_NEWDATE:
		return qvalue.QValueKindDate, nil
	case mysql.MYSQL_TYPE_VARCHAR:
		return qvalue.QValueKindString, nil
	case mysql.MYSQL_TYPE_BIT:
		return qvalue.QValueKindInt64, nil
	case mysql.MYSQL_TYPE_TIMESTAMP2:
		return qvalue.QValueKindTimestamp, nil
	case mysql.MYSQL_TYPE_DATETIME2:
		return qvalue.QValueKindTimestamp, nil
	case mysql.MYSQL_TYPE_TIME2:
		return qvalue.QValueKindTime, nil
	case mysql.MYSQL_TYPE_JSON:
		return qvalue.QValueKindJSON, nil
	case mysql.MYSQL_TYPE_NEWDECIMAL:
		return qvalue.QValueKindNumeric, nil
	case mysql.MYSQL_TYPE_ENUM:
		return qvalue.QValueKindInt64, nil
	case mysql.MYSQL_TYPE_SET:
		return qvalue.QValueKindInt64, nil
	case mysql.MYSQL_TYPE_TINY_BLOB:
		return qvalue.QValueKindBytes, nil
	case mysql.MYSQL_TYPE_MEDIUM_BLOB:
		return qvalue.QValueKindBytes, nil
	case mysql.MYSQL_TYPE_LONG_BLOB:
		return qvalue.QValueKindBytes, nil
	case mysql.MYSQL_TYPE_BLOB:
		return qvalue.QValueKindBytes, nil
	case mysql.MYSQL_TYPE_VAR_STRING:
		return qvalue.QValueKindString, nil
	case mysql.MYSQL_TYPE_STRING:
		return qvalue.QValueKindString, nil
	case mysql.MYSQL_TYPE_GEOMETRY:
		return qvalue.QValueKindGeometry, nil
	default:
		return qvalue.QValueKind(""), fmt.Errorf("unknown mysql type %d", ty)
	}
}

func QRecordSchemaFromMysqlFields(fields []*mysql.Field) (qvalue.QRecordSchema, error) {
	schema := make([]qvalue.QField, 0, len(fields))
	for _, field := range fields {
		qkind, err := qkindFromMysql(field.Type)
		if err != nil {
			return qvalue.QRecordSchema{}, err
		}

		schema = append(schema, qvalue.QField{
			Name:      string(field.Name),
			Type:      qkind,
			Precision: 0, // TODO numerics
			Scale:     0, // TODO numerics
			Nullable:  (field.Flag & mysql.NOT_NULL_FLAG) == 0,
		})
	}
	return qvalue.QRecordSchema{Fields: schema}, nil
}

func QValueFromMysqlFieldValue(qkind qvalue.QValueKind, fv mysql.FieldValue) (qvalue.QValue, error) {
	// TODO fill this in, maybe contribute upstream, figvure out how numeric etc fit in
	switch v := fv.Value().(type) {
	case nil:
		return qvalue.QValueNull(qkind), nil
	case uint64:
		// TODO unsigned integers
		switch qkind {
		case qvalue.QValueKindInt16:
			return qvalue.QValueInt16{Val: int16(v)}, nil
		case qvalue.QValueKindInt32:
			return qvalue.QValueInt32{Val: int32(v)}, nil
		case qvalue.QValueKindInt64:
			return qvalue.QValueInt64{Val: int64(v)}, nil
		default:
			return nil, fmt.Errorf("cannot convert uint64 to %s", qkind)
		}
	case int64:
		switch qkind {
		case qvalue.QValueKindInt16:
			return qvalue.QValueInt16{Val: int16(v)}, nil
		case qvalue.QValueKindInt32:
			return qvalue.QValueInt32{Val: int32(v)}, nil
		case qvalue.QValueKindInt64:
			return qvalue.QValueInt64{Val: v}, nil
		default:
			return nil, fmt.Errorf("cannot convert int64 to %s", qkind)
		}
	case float64:
		switch qkind {
		case qvalue.QValueKindFloat32:
			return qvalue.QValueFloat32{Val: float32(v)}, nil
		case qvalue.QValueKindFloat64:
			return qvalue.QValueFloat64{Val: float64(v)}, nil
		default:
			return nil, fmt.Errorf("cannot convert float64 to %s", qkind)
		}
	case []byte:
		switch qkind {
		case qvalue.QValueKindString:
			return qvalue.QValueString{Val: string(v)}, nil
		case qvalue.QValueKindBytes:
			return qvalue.QValueBytes{Val: v}, nil
		case qvalue.QValueKindJSON:
			return qvalue.QValueJSON{Val: string(v)}, nil
		case qvalue.QValueKindTimestamp:
			val, err := time.Parse("2006-01-02 15:04:05.000000", string(v))
			if err != nil {
				return nil, err
			}
			return qvalue.QValueTimestamp{Val: val}, nil
		case qvalue.QValueKindTime:
			val, err := time.Parse("15:04:05.000000", string(v))
			if err != nil {
				return nil, err
			}
			return qvalue.QValueTime{Val: val}, nil
		case qvalue.QValueKindDate:
			val, err := time.Parse(time.DateOnly, string(v))
			if err != nil {
				return nil, err
			}
			return qvalue.QValueDate{Val: val}, nil
		default:
			return nil, fmt.Errorf("cannot convert string %v to %s", v, qkind)
		}
	default:
		return nil, fmt.Errorf("unexpected mysql type %T", v)
	}
}
