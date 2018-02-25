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
	"fmt"
	"github.com/boltdb/bolt"
)

// MasterSlaveStatus of cluster
// undone
// Check out if any masters or slaves exists ant return masters, then slaves
//			MasterCluster, SlaveCluster := a.MasterSlaveClustersCheck
func (a *App) MasterSlaveClustersCheck() (MasterClusters, SlaveClusters []string) {
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
			//
			fmt.Printf("CLUSTERRRRRRRr  %v \n", cluster)
			fmt.Printf("CLUSTERSSSSSSSSS  %v \n", clusters)
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
	fmt.Printf("CHECKKKKKKKK 4 MasterCluster  %v \n", MasterClusters)
	fmt.Printf("CHECKKKKKKKK 4 SlaveCluster  %v \n", SlaveClusters)
	return

}
