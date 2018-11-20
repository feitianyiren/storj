// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package irrdbclient

import (
	"context"

	"google.golang.org/grpc"
	monkit "gopkg.in/spacemonkeygo/monkit.v2"
	"storj.io/storj/pkg/transport"

	"storj.io/storj/pkg/auth/grpcauth"
	"storj.io/storj/pkg/pb"
	"storj.io/storj/pkg/provider"
)

var (
	mon = monkit.Package()
)

// IrreparableDB creates a grpcClient
type IrreparableDB struct {
	client pb.IrrSegDBClient
}

// New Used as a public function
func New(gcclient pb.IrrSegDBClient) (irrdbc *IrreparableDB) {
	return &IrreparableDB{client: gcclient}
}

// a compiler trick to make sure *irrdbclient implements Client
var _ Client = (*IrreparableDB)(nil)

// Client services offerred for the interface
type Client interface {
	Put(ctx context.Context, putirrsegreq *pb.PutIrrSegRequest) error
}

// NewClient initializes a new irreparabledb client
func NewClient(identity *provider.FullIdentity, address string, APIKey string) (*IrreparableDB, error) {
	apiKeyInjector := grpcauth.NewAPIKeyInjector(APIKey)
	tc := transport.NewClient(identity)
	conn, err := tc.DialAddress(
		context.Background(),
		address,
		grpc.WithUnaryInterceptor(apiKeyInjector),
	)
	if err != nil {
		return nil, err
	}

	return &IrreparableDB{client: pb.NewIrrSegDBClient(conn)}, nil
}

// Put is used for creating a new entry or update and existing entry in the irreparable db
func (irrdb *IrreparableDB) Put(ctx context.Context, putReq *pb.PutIrrSegRequest) (err error) {
	defer mon.Task()(&ctx)(&err)

	_, err = irrdb.client.Put(ctx, putReq)

	return err
}
