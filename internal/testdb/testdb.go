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

	"github.com/romshark/conductor/db/dbpgx"
	"github.com/romshark/conductor/internal/backoff"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type Container struct {
	container testcontainers.Container
	host      string
	port      string
	adminDSN  string
}

func (c *Container) MustExec(sql string, arguments ...any) {
	ctx := context.Background()
	p, err := pgxpool.New(ctx, c.adminDSN)
	if err != nil {
		panic(err)
	}
	defer p.Close()
	_, err = p.Exec(ctx, sql, arguments...)
	if err != nil {
		panic(err)
	}
}

func (c *Container) MustStart(ctx context.Context) {
	if c.container != nil {
		panic("container already started")
	}

	req := testcontainers.ContainerRequest{
		Image: "postgres:17",
		Env: map[string]string{
			"POSTGRES_USER":     "testdb",
			"POSTGRES_PASSWORD": "testdb",
			"POSTGRES_DB":       "testdb",
		},
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForListeningPort("5432/tcp").WithStartupTimeout(60 * time.Second),
	}

	cont, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		panic(fmt.Errorf("starting container: %w", err))
	}

	host, err := cont.Host(ctx)
	if err != nil {
		panic(fmt.Errorf("container host: %w", err))
	}
	port, err := cont.MappedPort(ctx, "5432/tcp")
	if err != nil {
		panic(fmt.Errorf("container port: %w", err))
	}

	c.container = cont
	c.host = host
	c.port = port.Port()
	c.adminDSN = fmt.Sprintf("postgres://testdb:testdb@%s:%s/postgres?sslmode=disable",
		c.host, c.port)
}

func (c *Container) Terminate() {
	if c.container == nil {
		return
	}
	_ = c.container.Terminate(context.Background())
	c.container = nil
}

// NewDBPGX creates a new dbpgx based database within the container for test t.
func (c *Container) NewDBPGX(t testing.TB, log *slog.Logger) (*dbpgx.DB, string) {
	t.Helper()
	ctx := t.Context()

	if c.container == nil {
		t.Fatal("container not started")
	}

	// Derive name from test name.
	dbName := "test_" + strings.ReplaceAll(t.Name(), "/", "_")

	adminPool, err := pgxpool.New(ctx, c.adminDSN)
	require.NoError(t, err)
	defer adminPool.Close()

	dbNameSanitized := pgx.Identifier{dbName}.Sanitize()
	_, err = adminPool.Exec(ctx, `DROP DATABASE IF EXISTS `+dbNameSanitized)
	require.NoError(t, err)
	_, err = adminPool.Exec(ctx, `CREATE DATABASE `+dbNameSanitized)
	require.NoError(t, err)

	dsn := fmt.Sprintf("postgres://testdb:testdb@%s:%s/%s?sslmode=disable",
		c.host, c.port, dbName)

	bo, err := backoff.New(100*time.Millisecond, 300*time.Millisecond, 2.0, 0, nil)
	require.NoError(t, err)

	db, err := dbpgx.Open(ctx, log, dsn, 0, bo)
	require.NoError(t, err)

	t.Cleanup(db.Close)
	return db, dsn
}
