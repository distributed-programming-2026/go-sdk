package mysql

import (
	"errors"
	"time"

	// Register the MySQL driver for database/sql.
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Config struct {
	MaxConnections        int
	ConnectionMaxLifeTime time.Duration
	ConnectionMaxIdleTime time.Duration
}

type Connector interface {
	Open(dsn string, cfg Config) error
	Close() error
	TransactionalClient() TransactionalClient
}

func NewConnector() Connector { return &connector{} }

type connector struct{ db *sqlx.DB }

func (c *connector) Open(dsn string, cfg Config) error {
	var err error
	c.db, err = sqlx.Open("mysql", dsn)
	if err != nil {
		return err
	}
	c.db.SetMaxOpenConns(cfg.MaxConnections)
	c.db.SetConnMaxLifetime(cfg.ConnectionMaxLifeTime)
	c.db.SetConnMaxIdleTime(cfg.ConnectionMaxIdleTime)
	pingErr := c.db.Ping()
	if pingErr != nil {
		if err := c.db.Close(); err != nil {
			return err
		}
		return pingErr
	}
	return nil
}

func (c *connector) Close() error {
	if c.db == nil {
		return errors.New("db not initialized")
	}
	return c.db.Close()
}

func (c *connector) TransactionalClient() TransactionalClient {
	return NewTransactionalClientFromSQLx(c.db)
}
