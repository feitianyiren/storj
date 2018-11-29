// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package database

import dbx "storj.io/storj/pkg/database/dbx"

// RepairQueueDB interface for RepairQueueDB database operations
type RepairQueueDB interface {
}

type repairQueueDB struct {
	db *dbx.DB
}
