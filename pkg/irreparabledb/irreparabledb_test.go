// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package irreparabledb

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	dbx "storj.io/storj/pkg/irreparabledb/dbx"
	pb "storj.io/storj/pkg/irreparabledb/proto"
)

var (
	ctx = context.Background()
)

func TestCreateDoesNotExist(t *testing.T) {
	dbPath := getDBPath()
	irrdb, _, err := getServerAndDB(dbPath)
	assert.NoError(t, err)

	apiKey := []byte("")
	rmtsegkey := []byte("irreparableremotesegkey")
	rmtsegval := []byte("irreparableremotesegval")
	rmtseginfo := &pb.RmtSegInfo{
		RmtSegKey: rmtsegkey,
		RmtSegVal: rmtsegval,
	}
	createReq := &pb.CreateRequest{
		Rmtseginfo: rmtseginfo,
		APIKey:     apiKey,
	}
	resp, err := irrdb.Create(ctx, createReq)
	assert.NoError(t, err)
	status := resp.Status
	assert.EqualValues(t, 1, status)
}

func TestCreateExists(t *testing.T) {
	dbPath := getDBPath()
	irrdb, db, err := getServerAndDB(dbPath)
	assert.NoError(t, err)

	apiKey := []byte("")
	rmtsegkey := []byte("irreparableremotesegkey")
	rmtsegval := []byte("irreparableremotesegval")
	piecesLost := int64(10)
	damagedsegUnixSec := time.Now().Unix()
	repairAttemptCount := int64(10)

	err = createRmtSegInfo(ctx, db, rmtsegkey, rmtsegval, piecesLost, damagedsegUnixSec, repairAttemptCount)
	assert.NoError(t, err)

	rmtseginfo := &pb.RmtSegInfo{
		RmtSegKey:                rmtsegkey,
		RmtSegVal:                rmtsegval,
		RmtSegLostPiecesCount:    piecesLost,
		RmtSegRepairUnixSec:      damagedsegUnixSec,
		RmtSegRepairAttemptCount: repairAttemptCount,
	}
	createReq := &pb.CreateRequest{
		Rmtseginfo: rmtseginfo,
		APIKey:     apiKey,
	}

	_, err = irrdb.Create(ctx, createReq)
	assert.Error(t, err)
}

func TestCreateWithRmtSegInfo(t *testing.T) {
	dbPath := getDBPath()
	irrdb, db, err := getServerAndDB(dbPath)
	assert.NoError(t, err)

	apiKey := []byte("")
	rmtsegkey := []byte("irreparableremotesegkey")
	rmtsegval := []byte("irreparableremotesegval")
	piecesLost := int64(10)
	damagedsegUnixSec := time.Now().Unix()
	repairAttemptCount := int64(10)

	rmtseginfo := &pb.RmtSegInfo{
		RmtSegKey:                rmtsegkey,
		RmtSegVal:                rmtsegval,
		RmtSegLostPiecesCount:    piecesLost,
		RmtSegRepairUnixSec:      damagedsegUnixSec,
		RmtSegRepairAttemptCount: repairAttemptCount,
	}
	createReq := &pb.CreateRequest{
		Rmtseginfo: rmtseginfo,
		APIKey:     apiKey,
	}

	resp, err := irrdb.Create(ctx, createReq)
	assert.NoError(t, err)
	status := resp.Status
	assert.EqualValues(t, 1, status)

	dbrmtsegInfo, err := db.Get_Irreparabledb_By_Segmentkey(ctx, dbx.Irreparabledb_Segmentkey(rmtsegkey))
	assert.NoError(t, err)

	assert.EqualValues(t, rmtsegkey, dbrmtsegInfo.Segmentkey, rmtsegkey)
	assert.EqualValues(t, rmtsegval, dbrmtsegInfo.Segmentval, rmtsegval)
	assert.EqualValues(t, piecesLost, dbrmtsegInfo.PiecesLostCount, piecesLost)
	assert.EqualValues(t, damagedsegUnixSec, dbrmtsegInfo.SegDamagedUnixSec, damagedsegUnixSec)
	assert.EqualValues(t, repairAttemptCount, dbrmtsegInfo.RepairAttemptCount, repairAttemptCount)
}

// func TestGetExists(t *testing.T) {
// 	dbPath := getDBPath()
// 	statdb, db, err := getServerAndDB(dbPath)
// 	assert.NoError(t, err)

// 	apiKey := []byte("")
// 	nodeID := []byte("testnodeid")

// 	auditSuccessCount, totalAuditCount, auditRatio := getRatio(4, 10)
// 	uptimeSuccessCount, totalUptimeCount, uptimeRatio := getRatio(8, 25)

// 	err = createNode(ctx, db, nodeID, auditSuccessCount, totalAuditCount, auditRatio,
// 		uptimeSuccessCount, totalUptimeCount, uptimeRatio)
// 	assert.NoError(t, err)

// 	getReq := &pb.GetRequest{
// 		NodeId: nodeID,
// 		APIKey: apiKey,
// 	}
// 	resp, err := statdb.Get(ctx, getReq)
// 	assert.NoError(t, err)

// 	stats := resp.Stats
// 	assert.EqualValues(t, auditRatio, stats.AuditSuccessRatio)
// 	assert.EqualValues(t, uptimeRatio, stats.UptimeRatio)
// }

// func TestGetDoesNotExist(t *testing.T) {
// 	dbPath := getDBPath()
// 	statdb, _, err := getServerAndDB(dbPath)
// 	assert.NoError(t, err)

// 	apiKey := []byte("")
// 	nodeID := []byte("testnodeid")

// 	getReq := &pb.GetRequest{
// 		NodeId: nodeID,
// 		APIKey: apiKey,
// 	}
// 	_, err = statdb.Get(ctx, getReq)
// 	assert.Error(t, err)
// }

// func TestFindValidNodes(t *testing.T) {
// 	dbPath := getDBPath()
// 	statdb, db, err := getServerAndDB(dbPath)
// 	assert.NoError(t, err)

// 	apiKey := []byte("")

// 	for _, tt := range []struct {
// 		nodeID             []byte
// 		auditSuccessCount  int64
// 		totalAuditCount    int64
// 		auditRatio         float64
// 		uptimeSuccessCount int64
// 		totalUptimeCount   int64
// 		uptimeRatio        float64
// 	}{
// 		{[]byte("id1"), 10, 20, 0.5, 10, 20, 0.5},   // bad ratios
// 		{[]byte("id2"), 20, 20, 1, 20, 20, 1},       // good ratios
// 		{[]byte("id3"), 20, 20, 1, 10, 20, 0.5},     // good audit success bad uptime
// 		{[]byte("id4"), 10, 20, 0.5, 20, 20, 1},     // good uptime bad audit success
// 		{[]byte("id5"), 5, 5, 1, 5, 5, 1},           // good ratios not enough audits
// 		{[]byte("id6"), 20, 20, 1, 20, 20, 1},       // good ratios, excluded from query
// 		{[]byte("id7"), 19, 20, 0.95, 19, 20, 0.95}, // borderline ratios
// 	} {
// 		err = createNode(ctx, db, tt.nodeID, tt.auditSuccessCount, tt.totalAuditCount, tt.auditRatio,
// 			tt.uptimeSuccessCount, tt.totalUptimeCount, tt.uptimeRatio)
// 		assert.NoError(t, err)
// 	}

// 	findValidNodesReq := &pb.FindValidNodesRequest{
// 		NodeIds: [][]byte{
// 			[]byte("id1"), []byte("id2"),
// 			[]byte("id3"), []byte("id4"),
// 			[]byte("id5"), []byte("id7"),
// 		},
// 		MinStats: &pb.NodeStats{
// 			AuditSuccessRatio: 0.95,
// 			UptimeRatio:       0.95,
// 			AuditCount:        15,
// 		},
// 		APIKey: apiKey,
// 	}

// 	resp, err := statdb.FindValidNodes(ctx, findValidNodesReq)
// 	assert.NoError(t, err)

// 	passed := resp.PassedIds

// 	assert.Contains(t, passed, []byte("id2"))
// 	assert.Contains(t, passed, []byte("id7"))
// 	assert.Len(t, passed, 2)
// }

// func TestUpdateExists(t *testing.T) {
// 	dbPath := getDBPath()
// 	statdb, db, err := getServerAndDB(dbPath)
// 	assert.NoError(t, err)

// 	apiKey := []byte("")
// 	nodeID := []byte("testnodeid")

// 	auditSuccessCount, totalAuditCount, auditRatio := getRatio(4, 10)
// 	uptimeSuccessCount, totalUptimeCount, uptimeRatio := getRatio(8, 25)
// 	err = createNode(ctx, db, nodeID, auditSuccessCount, totalAuditCount, auditRatio,
// 		uptimeSuccessCount, totalUptimeCount, uptimeRatio)
// 	assert.NoError(t, err)

// 	node := &pb.Node{
// 		NodeId:             nodeID,
// 		UpdateAuditSuccess: true,
// 		AuditSuccess:       true,
// 		UpdateUptime:       true,
// 		IsUp:               false,
// 	}
// 	updateReq := &pb.UpdateRequest{
// 		Node:   node,
// 		APIKey: apiKey,
// 	}
// 	resp, err := statdb.Update(ctx, updateReq)
// 	assert.NoError(t, err)

// 	_, _, newAuditRatio := getRatio(int(auditSuccessCount+1), int(totalAuditCount+1))
// 	_, _, newUptimeRatio := getRatio(int(uptimeSuccessCount), int(totalUptimeCount+1))
// 	stats := resp.Stats
// 	assert.EqualValues(t, newAuditRatio, stats.AuditSuccessRatio)
// 	assert.EqualValues(t, newUptimeRatio, stats.UptimeRatio)
// }

// func TestUpdateBatchExists(t *testing.T) {
// 	dbPath := getDBPath()
// 	statdb, db, err := getServerAndDB(dbPath)
// 	assert.NoError(t, err)

// 	apiKey := []byte("")
// 	nodeID1 := []byte("testnodeid1")
// 	nodeID2 := []byte("testnodeid2")

// 	auditSuccessCount1, totalAuditCount1, auditRatio1 := getRatio(4, 10)
// 	uptimeSuccessCount1, totalUptimeCount1, uptimeRatio1 := getRatio(8, 25)
// 	err = createNode(ctx, db, nodeID1, auditSuccessCount1, totalAuditCount1, auditRatio1,
// 		uptimeSuccessCount1, totalUptimeCount1, uptimeRatio1)
// 	assert.NoError(t, err)
// 	auditSuccessCount2, totalAuditCount2, auditRatio2 := getRatio(7, 10)
// 	uptimeSuccessCount2, totalUptimeCount2, uptimeRatio2 := getRatio(8, 20)
// 	err = createNode(ctx, db, nodeID2, auditSuccessCount2, totalAuditCount2, auditRatio2,
// 		uptimeSuccessCount2, totalUptimeCount2, uptimeRatio2)
// 	assert.NoError(t, err)

// 	node1 := &pb.Node{
// 		NodeId:             nodeID1,
// 		UpdateAuditSuccess: true,
// 		AuditSuccess:       true,
// 		UpdateUptime:       true,
// 		IsUp:               false,
// 	}
// 	node2 := &pb.Node{
// 		NodeId:             nodeID2,
// 		UpdateAuditSuccess: true,
// 		AuditSuccess:       true,
// 		UpdateUptime:       false,
// 	}
// 	updateBatchReq := &pb.UpdateBatchRequest{
// 		NodeList: []*pb.Node{node1, node2},
// 		APIKey:   apiKey,
// 	}
// 	resp, err := statdb.UpdateBatch(ctx, updateBatchReq)
// 	assert.NoError(t, err)

// 	_, _, newAuditRatio1 := getRatio(int(auditSuccessCount1+1), int(totalAuditCount1+1))
// 	_, _, newUptimeRatio1 := getRatio(int(uptimeSuccessCount1), int(totalUptimeCount1+1))
// 	_, _, newAuditRatio2 := getRatio(int(auditSuccessCount2+1), int(totalAuditCount2+1))
// 	stats1 := resp.StatsList[0]
// 	stats2 := resp.StatsList[1]
// 	assert.EqualValues(t, newAuditRatio1, stats1.AuditSuccessRatio)
// 	assert.EqualValues(t, newUptimeRatio1, stats1.UptimeRatio)
// 	assert.EqualValues(t, newAuditRatio2, stats2.AuditSuccessRatio)
// 	assert.EqualValues(t, uptimeRatio2, stats2.UptimeRatio)
// }

// func TestUpdateBatchDoesNotExist(t *testing.T) {
// 	dbPath := getDBPath()
// 	statdb, db, err := getServerAndDB(dbPath)
// 	assert.NoError(t, err)

// 	apiKey := []byte("")
// 	nodeID1 := []byte("testnodeid1")
// 	nodeID2 := []byte("testnodeid2")

// 	auditSuccessCount1, totalAuditCount1, auditRatio1 := getRatio(4, 10)
// 	uptimeSuccessCount1, totalUptimeCount1, uptimeRatio1 := getRatio(8, 25)
// 	err = createNode(ctx, db, nodeID1, auditSuccessCount1, totalAuditCount1, auditRatio1,
// 		uptimeSuccessCount1, totalUptimeCount1, uptimeRatio1)
// 	assert.NoError(t, err)

// 	node1 := &pb.Node{
// 		NodeId:             nodeID1,
// 		UpdateAuditSuccess: true,
// 		AuditSuccess:       true,
// 		UpdateUptime:       true,
// 		IsUp:               false,
// 	}
// 	node2 := &pb.Node{
// 		NodeId:             nodeID2,
// 		UpdateAuditSuccess: true,
// 		AuditSuccess:       true,
// 		UpdateUptime:       false,
// 	}
// 	updateBatchReq := &pb.UpdateBatchRequest{
// 		NodeList: []*pb.Node{node1, node2},
// 		APIKey:   apiKey,
// 	}
// 	_, err = statdb.UpdateBatch(ctx, updateBatchReq)
// 	assert.NoError(t, err)
// }

// func TestUpdateBatchEmpty(t *testing.T) {
// 	dbPath := getDBPath()
// 	statdb, db, err := getServerAndDB(dbPath)
// 	assert.NoError(t, err)

// 	apiKey := []byte("")
// 	nodeID1 := []byte("testnodeid1")

// 	auditSuccessCount1, totalAuditCount1, auditRatio1 := getRatio(4, 10)
// 	uptimeSuccessCount1, totalUptimeCount1, uptimeRatio1 := getRatio(8, 25)
// 	err = createNode(ctx, db, nodeID1, auditSuccessCount1, totalAuditCount1, auditRatio1,
// 		uptimeSuccessCount1, totalUptimeCount1, uptimeRatio1)
// 	assert.NoError(t, err)

// 	updateBatchReq := &pb.UpdateBatchRequest{
// 		NodeList: []*pb.Node{},
// 		APIKey:   apiKey,
// 	}
// 	resp, err := statdb.UpdateBatch(ctx, updateBatchReq)
// 	assert.NoError(t, err)
// 	assert.Equal(t, len(resp.StatsList), 0)
// }

// func TestCreateEntryIfNotExists(t *testing.T) {
// 	dbPath := getDBPath()
// 	statdb, db, err := getServerAndDB(dbPath)
// 	assert.NoError(t, err)

// 	apiKey := []byte("")
// 	nodeID1 := []byte("testnodeid1")
// 	nodeID2 := []byte("testnodeid2")

// 	auditSuccessCount1, totalAuditCount1, auditRatio1 := getRatio(4, 10)
// 	uptimeSuccessCount1, totalUptimeCount1, uptimeRatio1 := getRatio(8, 25)
// 	err = createNode(ctx, db, nodeID1, auditSuccessCount1, totalAuditCount1, auditRatio1,
// 		uptimeSuccessCount1, totalUptimeCount1, uptimeRatio1)
// 	assert.NoError(t, err)

// 	node1 := &pb.Node{NodeId: nodeID1}
// 	createIfNotExistsReq1 := &pb.CreateEntryIfNotExistsRequest{
// 		Node:   node1,
// 		APIKey: apiKey,
// 	}
// 	_, err = statdb.CreateEntryIfNotExists(ctx, createIfNotExistsReq1)
// 	assert.NoError(t, err)

// 	nodeInfo1, err := db.Get_Node_By_Id(ctx, dbx.Node_Id(nodeID1))
// 	assert.NoError(t, err)
// 	assert.EqualValues(t, nodeID1, nodeInfo1.Id)
// 	assert.EqualValues(t, auditRatio1, nodeInfo1.AuditSuccessRatio)
// 	assert.EqualValues(t, uptimeRatio1, nodeInfo1.UptimeRatio)

// 	node2 := &pb.Node{NodeId: nodeID2}
// 	createIfNotExistsReq2 := &pb.CreateEntryIfNotExistsRequest{
// 		Node:   node2,
// 		APIKey: apiKey,
// 	}
// 	_, err = statdb.CreateEntryIfNotExists(ctx, createIfNotExistsReq2)
// 	assert.NoError(t, err)

// 	nodeInfo2, err := db.Get_Node_By_Id(ctx, dbx.Node_Id(nodeID2))
// 	assert.NoError(t, err)
// 	assert.EqualValues(t, nodeID2, nodeInfo2.Id)
// 	assert.EqualValues(t, 0, nodeInfo2.AuditSuccessRatio)
// 	assert.EqualValues(t, 0, nodeInfo2.UptimeRatio)
// }

func getDBPath() string {
	return fmt.Sprintf("file:memdb%d?mode=memory&cache=shared", rand.Int63())
}

func getServerAndDB(path string) (irreparabledb *Server, db *dbx.DB, err error) {
	irreparabledb, err = NewServer("sqlite3", path, zap.NewNop())
	if err != nil {
		return &Server{}, &dbx.DB{}, err
	}
	db, err = dbx.Open("sqlite3", path)
	if err != nil {
		return &Server{}, &dbx.DB{}, err
	}
	return irreparabledb, db, err
}

func createRmtSegInfo(ctx context.Context, db *dbx.DB, rmtsegkey []byte, rmtsegval []byte,
	piecesLost int64, damagedsegUnixSec int64, repairAttemptCount int64) error {
	_, err := db.Create_Irreparabledb(
		ctx,
		dbx.Irreparabledb_Segmentkey(rmtsegkey),
		dbx.Irreparabledb_Segmentval(rmtsegval),
		dbx.Irreparabledb_PiecesLostCount(piecesLost),
		dbx.Irreparabledb_SegDamagedUnixSec(damagedsegUnixSec),
		dbx.Irreparabledb_RepairAttemptCount(repairAttemptCount),
	)

	return err
}
