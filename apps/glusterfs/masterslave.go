//
// Copyright (c) 2015 The heketi Authors
//
// This file is licensed to you under your choice of the GNU Lesser
// General Public License, version 3 or any later version (LGPLv3 or
// later), or the GNU General Public License, version 2 (GPLv2), in all
// cases as published by the Free Software Foundation.
//
package glusterfs

import (
	//	"fmt"
	"github.com/boltdb/bolt"
	"github.com/chinacoolhacker/heketi/executors"
	"strings"
)

//SshdControl(a.executor, entry.Info.Id, start)
func (a *App) MasterSlaveSshdSet(action, clusterid string) error {
	logger.Debug("in Cluster %v action  %v \n", clusterid, action)
	err := a.db.View(func(tx *bolt.Tx) error {
		entry, err := NewClusterEntryFromId(tx, clusterid)
		if err == ErrNotFound {
			return err
		} else if err != nil {
			return err
		}
		logger.Debug("in Cluster %v nodes %v \n", clusterid, entry.Info.Nodes)

		for _, n := range entry.Info.Nodes {
			var newNode *NodeEntry
			newNode, err = NewNodeEntryFromId(tx, n)
			hostforexex := strings.Join(newNode.Info.Hostnames.Manage, ",")
			logger.Debug("in node %v \n", hostforexex)

			if err := executors.Executor.SshdControl(a.executor, hostforexex, action); err != nil {
				return err
			}

		}

		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// MasterSlaveStatus of cluster
// Check out if any masters or slaves exists ant return masters, then slaves
//			MasterCluster, SlaveCluster := a.MasterSlaveClustersCheck
func (a *App) MasterSlaveClustersCheck() (MasterClusters, SlaveClusters []string) {
	logger.Debug("In  MasterSlaveClustersCheck \n")
	var err error
	err = a.db.View(func(tx *bolt.Tx) error {
		clusters, err := ClusterList(tx)
		if err != nil {
			return err
		}
		if len(clusters) == 0 {
			return ErrNotFound
		}
		for _, cluster := range clusters {
			err := a.db.View(func(tx *bolt.Tx) error {
				entry, err := NewClusterEntryFromId(tx, cluster)
				if err == ErrNotFound {
					return err
				} else if err != nil {
					return err
				}

				if entry.Info.Status == "master" {
					logger.Debug("Cluster %v in master status \n", cluster)
					MasterClusters = []string{entry.Info.Id}
				}
				if entry.Info.Status == "slave" {
					logger.Debug("Cluster %v in slave status \n", cluster)
					SlaveClusters = []string{entry.Info.Id}
				}
				if err != nil {
					return err
				}
				return nil
			})

			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return
	}
	return

}
