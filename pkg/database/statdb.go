// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package database

import dbx "storj.io/storj/pkg/database/dbx"

// StatDB interface for StatDB database operations
type StatDB interface {
}

type statDB struct {
	db *dbx.DB
}
