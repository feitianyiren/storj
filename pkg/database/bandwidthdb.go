// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package database

import dbx "storj.io/storj/pkg/database/dbx"

// BandwidthAllocationDB interface for BandwidthAllocationDB database operations
type BandwidthAllocationDB interface {
}

type bandwidthAllocationDB struct {
	db *dbx.DB
}
