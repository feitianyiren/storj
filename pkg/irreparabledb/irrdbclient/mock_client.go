// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package irrdbclient

import (
	"context"

	"storj.io/storj/pkg/pb"
)

// MockIrreparableDB creates a noop Mock Irreparable Client
type MockIrreparableDB struct{}

// NewMockClient initializes a new mock Irreparable client
func NewMockClient() Client {
	return &MockIrreparableDB{}
}

// a compiler trick to make sure *MockIrreparableDB implements Client
var _ Client = (*MockIrreparableDB)(nil)

// Put is used for creating a new entry in the stats db with default reputation
func (irrdb *MockIrreparableDB) Put(ctx context.Context, req *pb.PutIrrSegRequest) (err error) {
	return nil
}
