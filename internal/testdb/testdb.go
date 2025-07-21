package testdb

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/romshark/conductor/db/dbpgx"
	"github.com/romshark/conductor/internal/backoff"
)

const (
	host     = "localhost"
	port     = "5432"
	user     = "testdb"
	password = "testdb"
	adminDB  = "postgres"
)

func AdminDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		user, password, host, port, adminDB)
}

// MustExecGlobal executes sql in the admin database.
func MustExecGlobal(sql string, args ...any) {
	adminPool, err := pgxpool.New(context.Background(), AdminDSN())
	if err != nil {
		panic(err)
	}
	defer adminPool.Close()
	_, err = adminPool.Exec(context.Background(), sql, args...)
	if err != nil {
		panic(err)
	}
}

// NewDBPGX creates a new dbpgx-based test database for the given test.
func NewDBPGX(t testing.TB, log *slog.Logger) (db *dbpgx.DB, dsn string) {
	t.Helper()
	ctx := t.Context()

	// Derive a unique test DB name from the test name
	dbName := "test_" + strings.ReplaceAll(t.Name(), "/", "_")

	adminPool, err := pgxpool.New(ctx, AdminDSN())
	require.NoError(t, err)
	defer adminPool.Close()

	dbNameSanitized := pgx.Identifier{dbName}.Sanitize()
	_, err = adminPool.Exec(ctx, `DROP DATABASE IF EXISTS `+dbNameSanitized)
	require.NoError(t, err)
	_, err = adminPool.Exec(ctx, `CREATE DATABASE `+dbNameSanitized)
	require.NoError(t, err)

	testDSN := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		user, password, host, port, dbName)

	bo, err := backoff.New(100*time.Millisecond, 300*time.Millisecond, 2.0, 0, nil)
	require.NoError(t, err)

	db, err = dbpgx.Open(ctx, log, testDSN, 0, bo)
	require.NoError(t, err)

	t.Cleanup(db.Close)
	return db, testDSN
}
