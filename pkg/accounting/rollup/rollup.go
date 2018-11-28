// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package rollup

import (
	"context"
	"time"

	"go.uber.org/zap"

	"storj.io/storj/pkg/accounting/accountingdb"
)

// Rollup is the service for totalling data on storage nodes for 1, 7, 30 day intervals
type Rollup interface {
	Run(ctx context.Context) error
}

type rollup struct {
	logger       *zap.Logger
	ticker       *time.Ticker
	accountingDB *accountingdb.Database
}

func newRollup(logger *zap.Logger, interval time.Duration) (*rollup, error) {
	db, err := accountingdb.New("", "") //TODO: what values go here?
	if err != nil {
		return nil, Error.Wrap(err)
	}
	return &rollup{
		logger:       logger,
		ticker:       time.NewTicker(interval),
		accountingDB: db,
	},  nil
}

// Run the rollup loop
func (r *rollup) Run(ctx context.Context) (err error) {
	defer mon.Task()(&ctx)(&err)

	for {
		err = r.Query(ctx)
		if err != nil {
			zap.L().Error("Rollup Query failed", zap.Error(err))
		}

		select {
		case <-r.ticker.C: // wait for the next interval to happen
		case <-ctx.Done(): // or the rollup is canceled via context
			return ctx.Err()
		}
	}
}

func (r *rollup) Query(ctx context.Context) error {
	return nil
}
