// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package overlay

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/zeebo/errs"

	"storj.io/storj/pkg/dht"
	"storj.io/storj/pkg/pb"
	"storj.io/storj/pkg/statdb"
	"storj.io/storj/pkg/storj"
	"storj.io/storj/storage"
)

const (
	// OverlayBucket is the string representing the bucket used for a bolt-backed overlay dht cache
	OverlayBucket = "overlay"
)

// ErrNodeNotFound error standardization
var ErrNodeNotFound = errs.New("Node not found")

// ErrBucketNotFound returns if a bucket is unable to be found in the routing table
var ErrBucketNotFound = errs.New("Bucket not found")

// OverlayError creates class of errors for stack traces
var OverlayError = errs.Class("Overlay Error")

// Cache is used to store overlay data in Redis
type Cache struct {
	DB     storage.KeyValueStore
	DHT    dht.DHT
	StatDB *statdb.Server
}

// NewOverlayCache returns a new Cache
func NewOverlayCache(db storage.KeyValueStore, dht dht.DHT, sdb *statdb.Server) *Cache {
	return &Cache{DB: db, DHT: dht, StatDB: sdb}
}

// Get looks up the provided nodeID from the overlay cache
func (o *Cache) Get(ctx context.Context, nodeID storj.NodeID) (*pb.Node, error) {
	b, err := o.DB.Get(nodeID.Bytes())
	if err != nil {
		return nil, err
	}
	if b.IsZero() {
		// TODO: log? return an error?
		return nil, nil
	}
	na := &pb.Node{}
	if err := proto.Unmarshal(b, na); err != nil {
		return nil, err
	}
	return na, nil
}

// GetAll looks up the provided nodeIDs from the overlay cache
func (o *Cache) GetAll(ctx context.Context, nodeIDs storj.NodeIDList) ([]*pb.Node, error) {
	if len(nodeIDs) == 0 {
		return nil, OverlayError.New("no nodeIDs provided")
	}
	var ks storage.Keys
	for _, v := range nodeIDs {
		ks = append(ks, v.Bytes())
	}
	vs, err := o.DB.GetAll(ks)
	if err != nil {
		return nil, err
	}
	var ns []*pb.Node
	for _, v := range vs {
		if v == nil {
			ns = append(ns, nil)
			continue
		}
		na := &pb.Node{}
		err := proto.Unmarshal(v, na)
		if err != nil {
			return nil, OverlayError.New("could not unmarshal non-nil node: %v", err)
		}
		ns = append(ns, na)
	}
	return ns, nil
}

// Put adds a nodeID to the redis cache with a binary representation of proto defined Node
func (o *Cache) Put(nodeID storj.NodeID, value pb.Node) error {
	// If we get a Node without an ID (i.e. bootstrap node)
	// we don't want to add to the routing tbale
	if nodeID == (storj.NodeID{}) {
		return nil
	}

	data, err := proto.Marshal(&value)
	if err != nil {
		return err
	}

	return o.DB.Put(nodeID.Bytes(), data)
}

// Bootstrap walks the initialized network and populates the cache
func (o *Cache) Bootstrap(ctx context.Context) error {
	// TODO(coyle): make Bootstrap work
	// look in our routing table
	// get every node we know about
	// ask every node for every node they know about
	// for each newly known node, ask those nodes for every node they know about
	// continue until no new nodes are found
	return nil
}

// Refresh updates the cache db with the current DHT.
// We currently do not penalize nodes that are unresponsive,
// but should in the future.
func (o *Cache) Refresh(ctx context.Context) error {
	// TODO(coyle): make refresh work by looking on the network for new ndoes
	nodes := o.DHT.Seen()

	for _, v := range nodes {
		if err := o.Put(v.Id, *v); err != nil {
			return err
		}
	}

	return nil
}

// Walk iterates over each node in each bucket to traverse the network
func (o *Cache) Walk(ctx context.Context) error {
	// TODO: This should walk the cache, rather than be a duplicate of refresh
	return nil
}

/*
// getNodeRep gets a node's stats from statdb and returns a node with reputation attached
func (o *Cache) getNodeRep(ctx context.Context, n *pb.Node) (*pb.Node, error) {
	stats, err := o.StatDB.CreateEntryIfNotExists(ctx, node.IDFromString(n.Id).Bytes())
	if err != nil {
		return nil, err
	}

	n.Reputation = &pb.NodeRep{
		UptimeRatio:       stats.UptimeRatio,
		AuditSuccessRatio: stats.AuditSuccessRatio,
		AuditCount:        stats.AuditCount,
	}

	return n, err
}

// pingNode pings a node, updates its uptime stats in statdb,
// and returns the node with reputation attached
func (o *Cache) pingNode(ctx context.Context, n *pb.Node) (*pb.Node, error) {
	id := node.ID(n.Id)

	pinged, err := o.DHT.Ping(ctx, *n)
	if err != nil {
		_, err := o.StatDB.UpdateUptime(ctx, id.Bytes(), false)
		if err != nil {
			return nil, err
		}
		return nil, err
	}

	stats, err := o.StatDB.UpdateUptime(ctx, id.Bytes(), true)
	if err != nil {
		return nil, err
	}

	pinged.Reputation = &pb.NodeRep{
		UptimeRatio:       stats.UptimeRatio,
		AuditSuccessRatio: stats.AuditSuccessRatio,
		AuditCount:        stats.AuditCount,
	}

	return &pinged, nil
}
*/
