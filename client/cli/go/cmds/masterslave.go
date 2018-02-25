// Copyright (c) 2015 The heketi Authors
//
// This file is licensed to you under your choice of the GNU Lesser
// General Public License, version 3 or any later version (LGPLv3 or
// later), or the GNU General Public License, version 2 (GPLv2), in all
// cases as published by the Free Software Foundation.
//

package cmds

import (
	"encoding/json"
	"errors"
	"fmt"

	client "github.com/chinacoolhacker/heketi/client/api/go-client"
	"github.com/chinacoolhacker/heketi/pkg/glusterfs/api"
	"github.com/spf13/cobra"
)

var (
	remoteId string
)

//func initMasterSlaveCommand() {
func init() {
	RootCmd.AddCommand(masterSlaveCommand)
	masterSlaveCommand.AddCommand(masterSlaveStatusCommand)
	clusterCommand.AddCommand(masterSlaveClusterCommand)
	masterSlaveClusterCommand.AddCommand(
		masterSlaveMasterCommand,
		masterSlaveSlaveCommand,
		masterSlaveClusterStatusCommand,
	)
	masterSlaveClusterCommand.PersistentFlags().StringVar(&remoteId, "remoteid", "", "Id of remote cluster")

}

var masterSlaveCommand = &cobra.Command{
	Use:   "masterslave",
	Short: "Heketi master-slave Management",
	Long:  "Heketi master-slave Management",
}

var masterSlaveStatusCommand = &cobra.Command{
	Use:   "status",
	Short: "master-slave status",
	Long:  "Displays master-slave status",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Create a client
		heketi := client.NewClient(options.Url, options.User, options.Key)
		// Get volume status
		status, err := heketi.MasterSlaveStatus()
		if err != nil {
			return err
		}

		if options.Json {
			data, err := json.Marshal(status)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, string(data))
		} else {
			fmt.Fprintf(stdout, "%v", status)
		}
		return nil
	},
}

var masterSlaveClusterCommand = &cobra.Command{
	Use:   "masterslave",
	Short: "Volume masterslave Management",
	Long:  "Heketi Cluster masterslave Management",
}

func actionMasterSlaveFunc(actionName string) func(*cobra.Command, []string) error {
	var action string
	var doneMsg string

	switch actionName {
	case "master":
		action = "master"
		doneMsg = "master \n"
	case "slave":
		action = "slave"
		doneMsg = "slave \n"
	default:
		return nil
	}

	return func(cmd *cobra.Command, args []string) error {
		//ensure proper number of args
		if len(cmd.Flags().Args()) < 1 {
			return errors.New("Cluster id missing")
		}

		clusterID := cmd.Flags().Arg(0)
		if clusterID == "" {
			return errors.New("Cluster id missing")
		}

		// Create a client
		heketi := client.NewClient(options.Url, options.User, options.Key)

		req := api.ClusterSetMasterSlaveRequest{
			MasterSlaveCluster: api.MasterSlaveCluster{
				Remoteid: remoteId,
				Status:   action,
			},
		}

		// Execute geo-replication action
		err := heketi.MasterClusterSlavePostAction(clusterID, &req)

		if err != nil {
			return err
		}

		fmt.Fprintf(stdout, doneMsg)

		return nil
	}
}

var masterSlaveMasterCommand = &cobra.Command{
	Use:     "master",
	Short:   "Master",
	Long:    "Switch selected cluster to master state",
	Example: " $ heketi-cli cluster masterslave --remoteid=8744f3aca77d43adee207f80f941b355 master be4d5e57b8f9ddae4cb2289d3dd4f235",
	RunE:    actionMasterSlaveFunc("master"),
}

var masterSlaveSlaveCommand = &cobra.Command{
	Use:     "slave",
	Short:   "Slave",
	Long:    "Switch selected cluster to master state",
	Example: " $ heketi-cli cluster masterslave --remoteid=8744f3aca77d43adee207f80f941b355 slave be4d5e57b8f9ddae4cb2289d3dd4f235",
	RunE:    actionMasterSlaveFunc("slave"),
}

var masterSlaveClusterStatusCommand = &cobra.Command{
	Use:     "status",
	Short:   "Status",
	Long:    "Show masterslave status of selected cluster ",
	Example: "  $ heketi-cli cluster masterslave status be4d5e57b8f9ddae4cb2289d3dd4f235",
	RunE: func(cmd *cobra.Command, args []string) error {
		//ensure proper number of args
		if len(cmd.Flags().Args()) < 1 {
			return errors.New("Cluster id missing")
		}

		clusterID := cmd.Flags().Arg(0)
		if clusterID == "" {
			return errors.New("Cluster id missing")
		}

		// Create a client
		heketi := client.NewClient(options.Url, options.User, options.Key)
		// Get volume status
		status, err := heketi.MasterSlaveClusterStatus(clusterID)
		if err != nil {
			return err
		}

		if options.Json {
			data, err := json.Marshal(status)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, string(data))
		} else {
			fmt.Fprintf(stdout, "%v", status)
		}
		return nil
	},
}

/*

var masterSlaveClusterStatusCommand = &cobra.Command{
	Use:   "status",
	Short: "geo-replication status",
	Long:  "Displays geo-replication status from the first node of the first cluster",
	RunE: func(cmd *cobra.Command, args []string) error {

		//ensure proper number of args
		if len(cmd.Flags().Args()) < 1 {
			return errors.New("Cluster id missing")
		}

		clusterID := cmd.Flags().Arg(0)
		if clusterID == "" {
			return errors.New("Cluster id missing")
		}

		// Create a client
		heketi := client.NewClient(options.Url, options.User, options.Key)

		req := ""

		fmt.Printf("req,  %v \n", req)
		fmt.Printf("ID,  %v \n", clusterID)
		fmt.Printf("ARGS,  %v \n", cmd.Flags())

		//MasterSlaveClusterStatus

		// Execute geo-replication action
		err := heketi.MasterSlaveClusterStatus(clusterID)

		fmt.Printf("ERR,  %v \n", err)
		if err != nil {
			return err
		}

		//		fmt.Fprintf(stdout, )

		return nil

	},
}
*/
