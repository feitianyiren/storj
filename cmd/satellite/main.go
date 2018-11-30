// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"text/tabwriter"

	"github.com/gogo/protobuf/proto"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs"

	"storj.io/storj/pkg/auth/grpcauth"
	"storj.io/storj/pkg/bwagreement"
	dbmanager "storj.io/storj/pkg/bwagreement/database-manager"
	"storj.io/storj/pkg/cfgstruct"
	"storj.io/storj/pkg/datarepair/checker"
	"storj.io/storj/pkg/datarepair/queue"
	"storj.io/storj/pkg/datarepair/repairer"
	"storj.io/storj/pkg/irreparabledb"
	"storj.io/storj/pkg/kademlia"
	"storj.io/storj/pkg/overlay"
	"storj.io/storj/pkg/pb"
	"storj.io/storj/pkg/pointerdb"
	"storj.io/storj/pkg/process"
	"storj.io/storj/pkg/provider"
	"storj.io/storj/pkg/statdb"
	"storj.io/storj/pkg/storj"
	"storj.io/storj/storage/redis"
)

var (
	rootCmd = &cobra.Command{
		Use:   "satellite",
		Short: "Satellite",
	}
	runCmd = &cobra.Command{
		Use:   "run",
		Short: "Run the satellite",
		RunE:  cmdRun,
	}
	setupCmd = &cobra.Command{
		Use:   "setup",
		Short: "Create config files",
		RunE:  cmdSetup,
	}
	diagCmd = &cobra.Command{
		Use:   "diag",
		Short: "Diagnostic Tool support",
		RunE:  cmdDiag,
	}
	qdiagCmd = &cobra.Command{
		Use:   "qdiag",
		Short: "Repair Queue Diagnostic Tool support",
		RunE:  cmdQDiag,
	}

	runCfg struct {
		Identity      provider.IdentityConfig
		Kademlia      kademlia.Config
		PointerDB     pointerdb.Config
		Overlay       overlay.Config
		StatDB        statdb.Config
		IrreparableDB irreparabledb.Config
		Checker       checker.Config
		Repairer      repairer.Config

		// Audit audit.Config
		BwAgreement bwagreement.Config
	}
	setupCfg struct {
		BasePath  string `default:"$CONFDIR" help:"base path for setup"`
		CA        provider.CASetupConfig
		Identity  provider.IdentitySetupConfig
		Overwrite bool `default:"false" help:"whether to overwrite pre-existing configuration files"`
	}
	diagCfg struct {
		DatabaseURL string `help:"the database connection string to use" default:"sqlite3://$CONFDIR/bw.db"`
	}
	qdiagCfg struct {
		DatabaseURL string `help:"the database connection string to use" default:"redis://127.0.0.1:6378?db=1&password=abc123"`
		QListLimit  int    `help:"maximum segments that can be requested" default:"1000"`
	}

	defaultConfDir = "$HOME/.storj/satellite"
)

func init() {
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(setupCmd)
	rootCmd.AddCommand(diagCmd)
	rootCmd.AddCommand(qdiagCmd)
	cfgstruct.Bind(runCmd.Flags(), &runCfg, cfgstruct.ConfDir(defaultConfDir))
	cfgstruct.Bind(setupCmd.Flags(), &setupCfg, cfgstruct.ConfDir(defaultConfDir))
	cfgstruct.Bind(diagCmd.Flags(), &diagCfg, cfgstruct.ConfDir(defaultConfDir))
	cfgstruct.Bind(qdiagCmd.Flags(), &qdiagCfg, cfgstruct.ConfDir(defaultConfDir))
}

func cmdRun(cmd *cobra.Command, args []string) (err error) {
	return runCfg.Identity.Run(
		process.Ctx(cmd),
		grpcauth.NewAPIKeyInterceptor(),
		runCfg.Kademlia,
		runCfg.PointerDB,
		runCfg.Overlay,
		runCfg.StatDB,
		runCfg.IrreparableDB,
		runCfg.Checker,
		runCfg.Repairer,
		// runCfg.Audit,
		runCfg.BwAgreement,
	)
}

func cmdSetup(cmd *cobra.Command, args []string) (err error) {
	setupCfg.BasePath, err = filepath.Abs(setupCfg.BasePath)
	if err != nil {
		return err
	}

	_, err = os.Stat(setupCfg.BasePath)
	if !setupCfg.Overwrite && err == nil {
		fmt.Println("An satellite configuration already exists. Rerun with --overwrite")
		return nil
	} else if setupCfg.Overwrite && err == nil {
		fmt.Println("overwriting existing satellite config")
		err = os.RemoveAll(setupCfg.BasePath)
		if err != nil {
			return err
		}
	}

	err = os.MkdirAll(setupCfg.BasePath, 0700)
	if err != nil {
		return err
	}

	// TODO: handle setting base path *and* identity file paths via args
	// NB: if base path is set this overrides identity and CA path options
	if setupCfg.BasePath != defaultConfDir {
		setupCfg.CA.CertPath = filepath.Join(setupCfg.BasePath, "ca.cert")
		setupCfg.CA.KeyPath = filepath.Join(setupCfg.BasePath, "ca.key")
		setupCfg.Identity.CertPath = filepath.Join(setupCfg.BasePath, "identity.cert")
		setupCfg.Identity.KeyPath = filepath.Join(setupCfg.BasePath, "identity.key")
	}
	err = provider.SetupIdentity(process.Ctx(cmd), setupCfg.CA, setupCfg.Identity)
	if err != nil {
		return err
	}

	o := map[string]interface{}{
		"identity.cert-path": setupCfg.Identity.CertPath,
		"identity.key-path":  setupCfg.Identity.KeyPath,
	}

	return process.SaveConfig(runCmd.Flags(),
		filepath.Join(setupCfg.BasePath, "config.yaml"), o)
}

func cmdDiag(cmd *cobra.Command, args []string) (err error) {
	// open the psql db
	u, err := url.Parse(diagCfg.DatabaseURL)
	if err != nil {
		return errs.New("Invalid Database URL: %+v", err)
	}

	dbm, err := dbmanager.NewDBManager(u.Scheme, u.Path)
	if err != nil {
		return err
	}

	//get all bandwidth aggrements rows already ordered
	baRows, err := dbm.GetBandwidthAllocations(context.Background())
	if err != nil {
		fmt.Printf("error reading satellite database %v: %v\n", u.Path, err)
		return err
	}

	// Agreement is a struct that contains a bandwidth agreement and the associated signature
	type UplinkSummary struct {
		TotalBytes        int64
		PutActionCount    int64
		GetActionCount    int64
		TotalTransactions int64
		// additional attributes add here ...
	}

	// attributes per uplinkid
	summaries := make(map[storj.NodeID]*UplinkSummary)
	uplinkIDs := storj.NodeIDList{}

	for _, baRow := range baRows {
		// deserializing rbad you get payerbwallocation, total & storage node id
		rbad := &pb.RenterBandwidthAllocation_Data{}
		if err := proto.Unmarshal(baRow.Data, rbad); err != nil {
			return err
		}

		// deserializing pbad you get satelliteID, uplinkID, max size, exp, serial# & action
		pbad := &pb.PayerBandwidthAllocation_Data{}
		if err := proto.Unmarshal(rbad.GetPayerAllocation().GetData(), pbad); err != nil {
			return err
		}

		uplinkID := pbad.UplinkId
		summary, ok := summaries[uplinkID]
		if !ok {
			summaries[uplinkID] = &UplinkSummary{}
			uplinkIDs = append(uplinkIDs, uplinkID)
			summary = summaries[uplinkID]
		}

		// fill the summary info
		summary.TotalBytes += rbad.GetTotal()
		summary.TotalTransactions++
		if pbad.GetAction() == pb.PayerBandwidthAllocation_PUT {
			summary.PutActionCount++
		} else {
			summary.GetActionCount++
		}
	}

	// initialize the table header (fields)
	const padding = 3
	w := tabwriter.NewWriter(os.Stdout, 0, 0, padding, ' ', tabwriter.AlignRight|tabwriter.Debug)
	fmt.Fprintln(w, "UplinkID\tTotal\t# Of Transactions\tPUT Action\tGET Action\t")

	// populate the row fields
	sort.Sort(uplinkIDs)
	for _, uplinkID := range uplinkIDs {
		summary := summaries[uplinkID]
		fmt.Fprint(w, uplinkID, "\t", summary.TotalBytes, "\t", summary.TotalTransactions, "\t", summary.PutActionCount, "\t", summary.GetActionCount, "\t\n")
	}

	// display the data
	return w.Flush()
}

func cmdQDiag(cmd *cobra.Command, args []string) (err error) {
	// open the redis db
	dbpath := qdiagCfg.DatabaseURL

	redisQ, err := redis.NewQueueFrom(dbpath)
	if err != nil {
		return err
	}

	queue := queue.NewQueue(redisQ)
	list, err := queue.Peekqueue(qdiagCfg.QListLimit)
	if err != nil {
		return err
	}

	// initialize the table header (fields)
	const padding = 3
	w := tabwriter.NewWriter(os.Stdout, 0, 0, padding, ' ', tabwriter.AlignRight|tabwriter.Debug)
	fmt.Fprintln(w, "Path\tLost Pieces\t")

	// populate the row fields
	for _, v := range list {
		fmt.Fprint(w, v.GetPath(), "\t", v.GetLostPieces(), "\t")
	}

	// display the data
	return w.Flush()
}

func main() {
	runCmd.Flags().String("config",
		filepath.Join(defaultConfDir, "config.yaml"), "path to configuration")
	process.Exec(rootCmd)
}
