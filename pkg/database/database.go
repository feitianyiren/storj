// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package database

import (
	"storj.io/storj/internal/migrate"
	"storj.io/storj/pkg/accounting"
	"storj.io/storj/pkg/bwagreement"
	dbx "storj.io/storj/pkg/database/dbx"
	"storj.io/storj/pkg/datarepair/queue"
	"storj.io/storj/pkg/overlay"
	"storj.io/storj/pkg/pointerdb"
	"storj.io/storj/pkg/statdb"
	"storj.io/storj/pkg/utils"
)

// DB contains access to different database tables
type DB struct {
	db *dbx.DB
}

// NewDB creates instance of database
func NewDB(databaseURL string) (*DB, error) {
	dbURL, err := utils.ParseURL(databaseURL)
	if err != nil {
		return nil, err
	}

	db, err := dbx.Open(dbURL.Scheme, databaseURL)
	if err != nil {
		return nil, err
	}

	return &DB{db: db}, nil
}

// PointerDB is a getter for PointerDB repository
func (db *DB) PointerDB() pointerdb.DB {
	return &pointerDB{db: db.db}
}

// StatDB is a getter for StatDB repository
func (db *DB) StatDB() statdb.DB {
	return &statDB{db: db.db}
}

// BandwidthAllocationDB is a getter for BandwidthAllocationDB repository
func (db *DB) BandwidthAllocationDB() bwagreement.DB {
	return &bandwidthAllocationDB{db: db.db}
}

// OverlayCacheDB is a getter for OverlayCacheDB repository
func (db *DB) OverlayCacheDB() overlay.DB {
	return &overlayCacheDB{db: db.db}
}

// RepairQueueDB is a getter for RepairQueueDB repository
func (db *DB) RepairQueueDB() queue.DB {
	return &repairQueueDB{db: db.db}
}

// AccountingDB is a getter for AccountingDB repository
func (db *DB) AccountingDB() accounting.DB {
	return &accountingDB{db: db.db}
}

// CreateTables is a method for creating all tables for database
func (db *DB) CreateTables() error {
	return migrate.Create("database", db.db)
}

// Close is used to close db connection
func (db *DB) Close() error {
	return db.db.Close()
}
