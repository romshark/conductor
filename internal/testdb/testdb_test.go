package testdb_test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/romshark/conductor/internal/testdb"

	"github.com/stretchr/testify/require"
)

var con testdb.Container

func TestMain(m *testing.M) {
	con.MustStart(context.Background())
	os.Exit(m.Run())
}

func TestNew(t *testing.T) {
	db, dsn := con.NewDBPGX(t, slog.Default())
	row := db.QueryRow(t.Context(), `SELECT '1';`)
	var val string
	err := row.Scan(&val)
	require.NoError(t, err)
	require.Equal(t, "1", val)
	require.NotEmpty(t, dsn)
}
