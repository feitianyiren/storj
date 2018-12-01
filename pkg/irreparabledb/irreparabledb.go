// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package irreparabledb

import (
	"context"

	"storj.io/storj/internal/migrate"
	dbx "storj.io/storj/pkg/irreparabledb/dbx"
	"storj.io/storj/pkg/utils"
)

// Database implements the irreparable RPC service
type Database struct {
	db     *dbx.DB
	driver string
}

// RemoteSegmentInfo is info about a single entry stored in the irreparable db
type RemoteSegmentInfo struct {
	EncryptedSegmentPath   []byte
	EncryptedSegmentDetail []byte //contains marshalled info of pb.Pointer
	LostPiecesCount        int64
	RepairUnixSec          int64
	RepairAttemptCount     int64
}

// New creates instance of Server
func New(source string) (*Database, error) {
	u, err := utils.ParseURL(source)
	if err != nil {
		return nil, err
	}

	db, err := dbx.Open(u.Scheme, u.Path)
	if err != nil {
		return nil, err
	}

	err = migrate.Create("irreparabledb", db)
	if err != nil {
		return nil, err
	}

	return &Database{
		db:     db,
		driver: u.Scheme,
	}, nil
}

// IncrementRepairAttempts a db entry for to increment the repair attempts field
func (db *Database) IncrementRepairAttempts(ctx context.Context, segmentInfo *RemoteSegmentInfo) (err error) {
	switch db.driver {
	case "postgres":
		querystr := "WITH upsert AS (UPDATE irreparabledbs SET repair_attempt_count=repair_attempt_count+1 WHERE segmentpath=? RETURNING *) INSERT INTO irreparabledbs  (segmentpath, segmentdetail, pieces_lost_count, seg_damaged_unix_sec, repair_attempt_count)  SELECT ?,?,?,?,? WHERE NOT EXISTS (SELECT * FROM upsert)"
		_, err = db.db.Exec(db.db.Rebind(querystr), segmentInfo.EncryptedSegmentPath, segmentInfo.EncryptedSegmentPath, segmentInfo.EncryptedSegmentDetail, segmentInfo.LostPiecesCount, segmentInfo.RepairUnixSec, segmentInfo.RepairAttemptCount)
	case "sqlite3":
		querystr := "INSERT INTO irreparabledbs (segmentpath, segmentdetail, pieces_lost_count, seg_damaged_unix_sec, repair_attempt_count) VALUES ( ?, ?, ?, ?, ? ) ON CONFLICT (segmentpath) DO UPDATE SET repair_attempt_count = repair_attempt_count + 1"
		_, err = db.db.Exec(db.db.Rebind(querystr), segmentInfo.EncryptedSegmentPath, segmentInfo.EncryptedSegmentDetail, segmentInfo.LostPiecesCount, segmentInfo.RepairUnixSec, segmentInfo.RepairAttemptCount)
	default:
		return Error.New("unsupported driver")
	}
	return err
}

// Get a irreparable's segment info from the db
func (db *Database) Get(ctx context.Context, segmentPath []byte) (resp *RemoteSegmentInfo, err error) {
	dbxInfo, err := db.db.Get_Irreparabledb_By_Segmentpath(ctx, dbx.Irreparabledb_Segmentpath(segmentPath))
	if err != nil {
		return &RemoteSegmentInfo{}, err
	}

	return &RemoteSegmentInfo{
		EncryptedSegmentPath:   dbxInfo.Segmentpath,
		EncryptedSegmentDetail: dbxInfo.Segmentdetail,
		LostPiecesCount:        dbxInfo.PiecesLostCount,
		RepairUnixSec:          dbxInfo.SegDamagedUnixSec,
		RepairAttemptCount:     dbxInfo.RepairAttemptCount,
	}, nil
}

// Delete a irreparable's segment info from the db
func (db *Database) Delete(ctx context.Context, segmentPath []byte) (err error) {
	_, err = db.db.Delete_Irreparabledb_By_Segmentpath(ctx, dbx.Irreparabledb_Segmentpath(segmentPath))

	return err
}
