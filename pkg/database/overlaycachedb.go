// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package database

import dbx "storj.io/storj/pkg/database/dbx"

type overlayCacheDB struct {
	db *dbx.DB
}
