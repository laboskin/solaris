package postgres

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/solarisdb/solaris/golibs/logging"
	"github.com/solarisdb/solaris/pkg/db"
	"time"
)

type (
	// Db exposes db operations
	Db struct {
		*sqlx.DB
		logger logging.Logger
	}
)

// MustGetDb does the same as GetDb but panics in case of an error
func MustGetDb(ctx context.Context, dbConn *db.DBConn) *Db {
	db, err := GetDb(ctx, dbConn)
	if err != nil {
		panic(err)
	}
	return db
}

// GetDb returns the Db object built for the given configuration
func GetDb(ctx context.Context, dbConn *db.DBConn) (*Db, error) {
	db, err := sqlx.ConnectContext(ctx, "postgres", dbConn.SourceName())
	if err != nil {
		return nil, fmt.Errorf("could not connect to the database: %w", err)
	}
	if dbConn.MaxConnections != nil {
		db.SetMaxOpenConns(*dbConn.MaxConnections)
	}
	if dbConn.MaxIdleConnections != nil {
		db.SetMaxIdleConns(*dbConn.MaxIdleConnections)
	}
	if dbConn.MaxConnIdleTimeSec != nil {
		db.SetConnMaxIdleTime(time.Duration(*dbConn.MaxConnIdleTimeSec) * time.Second)
	}
	if err = migrateUp(ctx, db.DB); err != nil {
		return nil, fmt.Errorf("migration failed: %w", err)
	}
	return &Db{DB: db}, nil
}

// Init implements linker.Initializer
func (s *Db) Init(ctx context.Context) error {
	s.logger = logging.NewLogger("db.postgres")
	s.logger.Infof("Initializing...")
	if err := migrateUp(ctx, s.DB.DB); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	return nil
}

// Shutdown implements linker.Shutdowner
func (s *Db) Shutdown() {
	s.logger.Infof("Shutting down...")
	if s.DB != nil {
		_ = s.DB.Close()
	}
}
