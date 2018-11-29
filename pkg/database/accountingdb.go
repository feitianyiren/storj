// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package database

import dbx "storj.io/storj/pkg/database/dbx"

// AccountingDB interface for AccountingDB database operations
type AccountingDB interface {
}

type accountingDB struct {
	db *dbx.DB
}
