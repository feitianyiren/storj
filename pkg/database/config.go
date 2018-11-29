// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package database

import (
	"context"

	"storj.io/storj/pkg/provider"
)

//CtxKey Used as database key
type CtxKey int

const (
	ctxKeyStats CtxKey = iota
)

// Config is a configuration struct that is everything you need to start database
type Config struct {
	DatabaseURL string `help:"the database connection string to use" default:"postgres://postgres@localhost/storj?sslmode=disable"`
}

// Run implements the provider.Responsibility interface
func (c Config) Run(ctx context.Context, server *provider.Provider) error {
	database, err := NewDB(c.DatabaseURL)
	if err != nil {
		return err
	}

	err = database.CreateTables()
	if err != nil {
		return err
	}

	return server.Run(context.WithValue(ctx, ctxKeyStats, database))
}

// LoadFromContext loads an existing database from the context if exists.
func LoadFromContext(ctx context.Context) *DB {
	if v, ok := ctx.Value(ctxKeyStats).(*DB); ok {
		return v
	}
	return nil
}
