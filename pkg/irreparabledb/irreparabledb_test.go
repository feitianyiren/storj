// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package irreparabledb

import (
	"context"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"storj.io/storj/internal/testcontext"
)

const (
	// this connstring is expected to work under the storj-test docker-compose instance
	defaultPostgresConn = "postgres://storj:storj-pass@test-postgres/teststorj?sslmode=disable"
)

var (
	testPostgres = flag.String("postgres-test-db", os.Getenv("STORJ_POSTGRES_TEST"), "PostgreSQL test database connection string")
)

func TestPostgres(t *testing.T) {
	ctx := testcontext.New(t)
	defer ctx.Cleanup()

	if *testPostgres == "" {
		t.Skipf("postgres flag missing, example:\n-postgres-test-db=%s", defaultPostgresConn)
	}

	// creating in-memory db and opening connection
	irrdb, err := New("postgres://kishore@localhost/postgres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer ctx.Check(irrdb.db.Close)

	testDatabase(t, ctx, irrdb)
}

func TestSqlite3(t *testing.T) {
	ctx := testcontext.New(t)
	defer ctx.Cleanup()

	// creating in-memory db and opening connection
	irrdb, err := New("sqlite3://file::memory:?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer ctx.Check(irrdb.db.Close)

	testDatabase(t, ctx, irrdb)
}

func testDatabase(t *testing.T, ctx context.Context, irrdb *Database) {
	//testing variables
	segmentInfo := &RemoteSegmentInfo{
		EncryptedSegmentPath:   []byte("IamSegmentkeyinfo"),
		EncryptedSegmentDetail: []byte("IamSegmentdetailinfo"),
		LostPiecesCount:        int64(10),
		RepairUnixSec:          time.Now().Unix(),
		RepairAttemptCount:     int64(10),
	}

	{ // New entry
		err := irrdb.IncrementRepairAttempts(ctx, segmentInfo)
		assert.NoError(t, err)
	}

	{ //Increment the already existing entry
		err := irrdb.IncrementRepairAttempts(ctx, segmentInfo)
		assert.NoError(t, err)
		segmentInfo.RepairAttemptCount++

		dbxInfo, err := irrdb.Get(ctx, segmentInfo.EncryptedSegmentPath)
		assert.NoError(t, err)
		assert.Equal(t, segmentInfo, dbxInfo)
	}

	{ //Delete existing entry
		err := irrdb.Delete(ctx, segmentInfo.EncryptedSegmentPath)
		assert.NoError(t, err)

		_, err = irrdb.Get(ctx, segmentInfo.EncryptedSegmentPath)
		assert.Error(t, err)
	}
}
