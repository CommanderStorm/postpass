package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"gotest.tools/v3/assert"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// setupTestDatabase initializes a PostGIS container for testing
func setupTestDatabase(t *testing.T) (testcontainers.Container, *sql.DB, context.Context) {
	ctx := context.Background()
	container, err := postgres.Run(ctx,
		"postgis/postgis:17-3.5-alpine",
		postgres.WithUsername("readonly"),
		postgres.WithPassword("readonly"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	assert.NilError(t, err, "Could not start container")

	host, err := container.Host(ctx)
	assert.NilError(t, err)
	port, err := container.MappedPort(ctx, "5432")
	assert.NilError(t, err)

	// Connect to database
	dbConnStr := fmt.Sprintf("host=%s port=%s user=readonly password=readonly dbname=postgres sslmode=disable", host, port.Port())
	db, err := sql.Open("postgres", dbConnStr)
	assert.NilError(t, err, "Could not connect to database")
	assert.NilError(t, db.Ping())
	return container, db, ctx
}

func initDb(db *sql.DB, t *testing.T) {
	_, err := db.Exec(`CREATE TABLE test_table (id SERIAL PRIMARY KEY, geom Geometry, name text);`)
	assert.NilError(t, err, "Could not initialize test table")
	_, err = db.Exec(`INSERT INTO test_table (geom,name) VALUES ('Test Entry', 'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))');`)
	assert.NilError(t, err, "Could not initialize test table")
}

// TestWorker verifies that the API correctly executes SQL queries
func TestWorker(t *testing.T) {
	// Setup test PostGIS container
	container, db, ctx := setupTestDatabase(t)
	defer func() {
		err := container.Terminate(ctx)
		assert.NilError(t, err)
	}()
	initDb(db, t)

	// Prepare worker channels
	tasks := make(chan WorkItem, 1)

	// Start a worker
	go worker(db, 1, tasks)

	// simple SELECT query
	response := make(chan SqlResponse, 1)
	tasks <- WorkItem{
		request:  "SELECT 'test'",
		response: response,
	}
	res := <-response
	assert.Assert(t, !res.err)
	assert.DeepEqual(t, res.result, "test")

	// invalid SQL query
	response = make(chan SqlResponse, 1)
	tasks <- WorkItem{
		request:  "SELECT * FROM non_existent_table",
		response: response,
	}
	res = <-response
	assert.Assert(t, res.err)
	assert.DeepEqual(t, res.result, `pq: relation "non_existent_table" does not exist`)
}
