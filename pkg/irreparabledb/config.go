// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package irreparabledb

import (
	"context"

	"go.uber.org/zap"

	"storj.io/storj/pkg/pb"
	"storj.io/storj/pkg/provider"
)

// CtxKeyIrreparabledb Used as pointerdb key
type CtxKeyIrreparabledb int

const (
	ctxKey CtxKeyIrreparabledb = iota
)

// Config is a configuration struct that is everything you need to start a
// irreparabale segment db responsibility
type Config struct {
	DatabaseURL    string `help:"the database connection string to use" default:"$CONFDIR/irreparable.db"`
	DatabaseDriver string `help:"the database driver to use" default:"sqlite3"`
}

// Run implements the provider.Responsibility interface
func (c Config) Run(ctx context.Context, server *provider.Provider) error {
	ns, err := NewServer(c.DatabaseDriver, c.DatabaseURL, zap.L())
	if err != nil {
		return err
	}

	pb.RegisterIrrSegDBServer(server.GRPC(), ns)

	return server.Run(ctx)
}

// LoadFromContext gives access to the irreparabledb server from the context, or returns nil
func LoadFromContext(ctx context.Context) *Server {
	if v, ok := ctx.Value(ctxKey).(*Server); ok {
		return v
	}
	return nil
}
