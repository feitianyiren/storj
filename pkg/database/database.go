// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package database

import (
	"storj.io/storj/internal/migrate"
	dbx "storj.io/storj/pkg/database/dbx"
	"storj.io/storj/pkg/utils"
)

// DB contains access to different database tables
type DB interface {
	// PointerDB is a getter for PointerDB repository
	PointerDB() PointerDB
	// StatDB is a getter for StatDB repository
	StatDB() StatDB
	// BandwidthAllocationDB is a getter for BandwidthAllocationDB repository
	BandwidthAllocationDB() BandwidthAllocationDB
	// OverlayCacheDB is a getter for OverlayCacheDB repository
	OverlayCacheDB() OverlayCacheDB
	// RepairQueueDB is a getter for RepairQueueDB repository
	RepairQueueDB() RepairQueueDB
	// AccountingDB is a getter for AccountingDB repository
	AccountingDB() AccountingDB
	// SatelliteDB is a getter for SatelliteDB repository
	// SatelliteDB() SatelliteDB

	// CreateTables is a method for creating all tables for database
	CreateTables() error
	// Close is used to close db connection
	Close() error
}

type database struct {
	db *dbx.DB

	DB
}

// NewDB creates instance of database
func NewDB(databaseURL string) (DB, error) {
	dbURL, err := utils.ParseURL(databaseURL)
	if err != nil {
		return nil, err
	}

	db, err := dbx.Open(dbURL.Scheme, databaseURL)
	if err != nil {
		return nil, err
	}

	return &database{db: db}, nil
}

// PointerDB is a getter for PointerDB repository
func (db *database) PointerDB() PointerDB {
	return &pointerDB{db: db.db}
}

// StatDB is a getter for StatDB repository
func (db *database) StatDB() StatDB {
	return &statDB{db: db.db}
}

// BandwidthAllocationDB is a getter for BandwidthAllocationDB repository
func (db *database) BandwidthAllocationDB() BandwidthAllocationDB {
	return &bandwidthAllocationDB{db: db.db}
}

// OverlayCacheDB is a getter for OverlayCacheDB repository
func (db *database) OverlayCacheDB() OverlayCacheDB {
	return &overlayCacheDB{db: db.db}
}

// RepairQueueDB is a getter for RepairQueueDB repository
func (db *database) RepairQueueDB() RepairQueueDB {
	return &repairQueueDB{db: db.db}
}

// AccountingDB is a getter for AccountingDB repository
func (db *database) AccountingDB() AccountingDB {
	return &accountingDB{db: db.db}
}

// CreateTables is a method for creating all tables for database
func (db *database) CreateTables() error {
	return migrate.Create("database", db.db)
}

// Close is used to close db connection
func (db *database) Close() error {
	return db.db.Close()
}
