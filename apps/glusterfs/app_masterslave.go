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
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/chinacoolhacker/heketi/pkg/glusterfs/api"
	"github.com/gorilla/mux"
	"github.com/heketi/utils"
	"net/http"
)

// MasterSlaveStatus of cluster
func (a *App) MasterSlaveClustersStatus(w http.ResponseWriter, r *http.Request) {
	logger.Debug("In MasterSlaveClustersStatus")

	MasterClusters, SlaveClusters := a.MasterSlaveClustersCheck()
	logger.Debug("MasterClusters is %v and SlaveClusters is %v \n", MasterClusters, SlaveClusters)

	// Get the id from the URL
	vars := mux.Vars(r)
	id := vars["id"]

	// Get info from db
	var info *api.ClusterInfoResponse
	err := a.db.View(func(tx *bolt.Tx) error {

		// Create a db entry from the id
		entry, err := NewClusterEntryFromId(tx, id)
		fmt.Printf("E %v\n", entry)
		if err == ErrNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return err
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}

		// Create a response from the db entry
		info, err = entry.NewClusterInfoResponse(tx)
		if info.Remoteid == "" {
			logger.LogError("No MasterSlave configured yet: %v \n", err)
			return err
		}
		if err != nil {
			return err
		}
		err = UpdateClusterInfoComplete(tx, info)
		if err != nil {
			return err
		}
		logger.Debug("Id is %v, Remid is %v Status is %v Volumes is %v", info.Id, info.Remoteid, info.Status, info.Volumes)

		return nil
	})
	if err != nil {
		return
	}

	// Write msg
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(info); err != nil {
		panic(err)
	}
}

func (a *App) MasterSlaveClusterPostHandler(w http.ResponseWriter, r *http.Request) {
	logger.Debug("In MasterSlaveClusterPostHandler")
	var msg api.ClusterSetMasterSlaveRequest

	// Get the id from the URL
	vars := mux.Vars(r)
	id := vars["id"]

	err := utils.GetJsonFromRequest(r, &msg)
	if err != nil {
		http.Error(w, "request unable to be parsed", 422)
		return
	}

	err = a.db.Update(func(tx *bolt.Tx) error {
		if err == ErrNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return err
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}

		entry, err := NewClusterEntryFromId(tx, id)
		if msg.Remoteid != "" {
			entry.Info.Remoteid = msg.Remoteid
		}

		rementry, err := NewClusterEntryFromId(tx, entry.Info.Remoteid)
		rementry.Info.Remoteid = id

		//        MasterCluster, SlaveCluster = a.MasterSlaveClustersCheck()

		switch msg.Status {
		case "master":
			entry.Info.Status = msg.Status
			rementry.Info.Status = "slave"

			// Enslave rem cluster
			for _, volume := range rementry.Info.Volumes {
				volumeEntry, err := NewVolumeEntryFromId(tx, volume)

				if volumeEntry.Info.Remvolid != "" {
					var host string

					remvolumeEntry, err := NewVolumeEntryFromId(tx, volumeEntry.Info.Remvolid)

					if err != nil {
						return err
					}

					a.asyncManager.AsyncHttpRedirectFunc(w, r, func() (string, error) {

						actionParams := make(map[string]string)
						actionParams["option"] = "push-pem"
						actionParams["force"] = "true"

						err = a.db.View(func(tx *bolt.Tx) error {
							node, err := NewNodeEntryFromId(tx, rementry.Info.Nodes[0])
							if err == ErrNotFound {
								logger.LogError("Node Id not found: %v", err)
								return err
							} else if err != nil {
								logger.LogError("Internal error: %v", err)
								return err
							}

							host = node.ManageHostName()

							return nil
						})

						geoRepStartRequest := api.GeoReplicationRequest{
							Action: api.GeoReplicationActionStop,
							GeoReplicationInfo: api.GeoReplicationInfo{
								SlaveHost:   remvolumeEntry.Info.Mount.GlusterFS.Hosts[0],
								SlaveVolume: remvolumeEntry.Info.Name,
							},
						}

						//todo: should be performed after volume create
						logger.Debug("Start geo replicate with request %v \n", geoRepStartRequest)

						if err := volumeEntry.GeoReplicationAction(a.db, a.executor, host, geoRepStartRequest); err != nil {
							return "", err
						}

						logger.Info("Geo-Replication is started for volume: %v \n", volumeEntry)

						return "/volumes/" + volumeEntry.Info.Id + "/georeplication", nil
					})

				}

				if err != nil {
					return err
				}
			}
			//enslaved

			// stop sshd on current cluster
			err = a.MasterSlaveSshdSet("stop", entry.Info.Id)
			if err != nil {
				return err
			}

			// start sshd on cluster which will be a slave
			err = a.MasterSlaveSshdSet("start", rementry.Info.Id)
			if err != nil {
				return err
			}

			// emnaster cur clus
			for _, volume := range entry.Info.Volumes {
				volumeEntry, err := NewVolumeEntryFromId(tx, volume)

				if volumeEntry.Info.Remvolid != "" {
					var host string

					remvolumeEntry, err := NewVolumeEntryFromId(tx, volumeEntry.Info.Remvolid)

					if err != nil {
						return err
					}

					a.asyncManager.AsyncHttpRedirectFunc(w, r, func() (string, error) {

						actionParams := make(map[string]string)
						actionParams["option"] = "push-pem"
						actionParams["force"] = "true"

						err = a.db.View(func(tx *bolt.Tx) error {
							node, err := NewNodeEntryFromId(tx, entry.Info.Nodes[0])
							if err == ErrNotFound {
								logger.LogError("Node Id not found: %v", err)
								return err
							} else if err != nil {
								logger.LogError("Internal error: %v", err)
								return err
							}

							host = node.ManageHostName()

							return nil
						})

						geoRepStartRequest := api.GeoReplicationRequest{
							Action: api.GeoReplicationActionStart,
							GeoReplicationInfo: api.GeoReplicationInfo{
								SlaveHost:   remvolumeEntry.Info.Mount.GlusterFS.Hosts[0],
								SlaveVolume: remvolumeEntry.Info.Name,
							},
						}

						//todo: should be performed after volume create
						logger.Debug("Start geo replicate with request %v \n", geoRepStartRequest)

						if err := volumeEntry.GeoReplicationAction(a.db, a.executor, host, geoRepStartRequest); err != nil {
							return "", err
						}

						logger.Info("Geo-Replication is started for volume: %v \n", volumeEntry)

						return "/volumes/" + volumeEntry.Info.Id + "/georeplication", nil
					})

				}

				if err != nil {
					return err
				}
				//  enmastered
			}

			//			entry.MasterSlaveEnmaster // startup georep from master to slave
			//
		case "slave":
			entry.Info.Status = msg.Status
			rementry.Info.Status = "master"
			// stop this cluster
			// Enslave rem cluster
			for _, volume := range entry.Info.Volumes {
				volumeEntry, err := NewVolumeEntryFromId(tx, volume)

				if volumeEntry.Info.Remvolid != "" {
					var host string

					remvolumeEntry, err := NewVolumeEntryFromId(tx, volumeEntry.Info.Remvolid)

					if err != nil {
						return err
					}

					a.asyncManager.AsyncHttpRedirectFunc(w, r, func() (string, error) {

						actionParams := make(map[string]string)
						actionParams["option"] = "push-pem"
						actionParams["force"] = "true"

						err = a.db.View(func(tx *bolt.Tx) error {
							node, err := NewNodeEntryFromId(tx, entry.Info.Nodes[0])
							if err == ErrNotFound {
								logger.LogError("Node Id not found: %v", err)
								return err
							} else if err != nil {
								logger.LogError("Internal error: %v", err)
								return err
							}

							host = node.ManageHostName()

							return nil
						})

						geoRepStartRequest := api.GeoReplicationRequest{
							Action: api.GeoReplicationActionStop,
							GeoReplicationInfo: api.GeoReplicationInfo{
								SlaveHost:   remvolumeEntry.Info.Mount.GlusterFS.Hosts[0],
								SlaveVolume: remvolumeEntry.Info.Name,
							},
						}

						//todo: should be performed after volume create
						logger.Debug("Start geo replicate with request %v \n", geoRepStartRequest)

						if err := volumeEntry.GeoReplicationAction(a.db, a.executor, host, geoRepStartRequest); err != nil {
							return "", err
						}

						logger.Info("Geo-Replication is started for volume: %v \n", volumeEntry)

						return "/volumes/" + volumeEntry.Info.Id + "/georeplication", nil
					})

				}

				if err != nil {
					return err
				}
			}
			//enslaved

			// stop sshd on cluster which will be master - rem cluster
			err = a.MasterSlaveSshdSet("stop", rementry.Info.Id)
			if err != nil {
				return err
			}
			// start sshd on current cluster which will be slave - this cluster
			err = a.MasterSlaveSshdSet("start", entry.Info.Id)
			if err != nil {
				return err
			}

			//rementry.MasterSlaveEnmaster

			// emnaster rem cluster
			for _, volume := range rementry.Info.Volumes {
				volumeEntry, err := NewVolumeEntryFromId(tx, volume)

				if volumeEntry.Info.Remvolid != "" {
					var host string

					remvolumeEntry, err := NewVolumeEntryFromId(tx, volumeEntry.Info.Remvolid)

					if err != nil {
						return err
					}

					a.asyncManager.AsyncHttpRedirectFunc(w, r, func() (string, error) {

						actionParams := make(map[string]string)
						actionParams["option"] = "push-pem"
						actionParams["force"] = "true"

						err = a.db.View(func(tx *bolt.Tx) error {
							node, err := NewNodeEntryFromId(tx, rementry.Info.Nodes[0])
							if err == ErrNotFound {
								logger.LogError("Node Id not found: %v", err)
								return err
							} else if err != nil {
								logger.LogError("Internal error: %v", err)
								return err
							}

							host = node.ManageHostName()

							return nil
						})

						geoRepStartRequest := api.GeoReplicationRequest{
							Action: api.GeoReplicationActionStart,
							GeoReplicationInfo: api.GeoReplicationInfo{
								SlaveHost:   remvolumeEntry.Info.Mount.GlusterFS.Hosts[0],
								SlaveVolume: remvolumeEntry.Info.Name,
							},
						}

						//todo: should be performed after volume create
						logger.Debug("Start geo replicate with request %v \n", geoRepStartRequest)

						if err := volumeEntry.GeoReplicationAction(a.db, a.executor, host, geoRepStartRequest); err != nil {
							return "", err
						}

						logger.Info("Geo-Replication is started for volume: %v \n", volumeEntry)

						return "/volumes/" + volumeEntry.Info.Id + "/georeplication", nil
					})

				}

				if err != nil {
					return err
				}
				//  enmastered
			}

		default:
			logger.LogError("Status %v invalid - use master or slave \n", msg.Status)
			return nil
		}

		err = entry.Save(tx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}
		err = rementry.Save(tx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}

		return nil
	})
	if err != nil {
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
}

// status total iterated by vol
func (a *App) MasterSlaveStatus(w http.ResponseWriter, r *http.Request) {

	var clusters []string
	var volumes []api.MasterSlaveVolpair

	// Get all the cluster ids from the DB
	err := a.db.View(func(tx *bolt.Tx) error {
		var err error

		clusters, err = ClusterList(tx)

		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		logger.Err(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, id := range clusters {
		var info *api.ClusterInfoResponse

		err := a.db.View(func(tx *bolt.Tx) error {

			entry, err := NewClusterEntryFromId(tx, id)

			if err == ErrNotFound {
				http.Error(w, err.Error(), http.StatusNotFound)
				return err
			} else if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return err
			}
			//dbg

			// Create a response from the db entry
			if entry.Info.Status == "master" {

				logger.Debug("Cluster %v found as %v \n", entry.Info.Id, entry.Info.Status)

				info, err = entry.NewClusterInfoResponse(tx)
				if err != nil {
					return err
				}
				err = UpdateClusterInfoComplete(tx, info)
				if err != nil {
					return err
				}

				for _, id := range info.Volumes {

					var vol *api.VolumeInfoResponse
					err := a.db.View(func(tx *bolt.Tx) error {
						entry, err := NewVolumeEntryFromId(tx, id)
						if err == ErrNotFound || !entry.Visible() {
							// treat an invisible entry like it doesn't exist
							http.Error(w, "Id not found", http.StatusNotFound)
							return ErrNotFound
						} else if err != nil {
							http.Error(w, err.Error(), http.StatusInternalServerError)
							return err
						}

						vol, err = entry.NewInfoResponse(tx)
						if err != nil {
							http.Error(w, err.Error(), http.StatusInternalServerError)
							return err
						}

						volpair := api.MasterSlaveVolpair{
							Id:       vol.Id,
							Name:     vol.Name,
							Remvolid: vol.Remvolid,
						}

						logger.Debug("Volume pair %v found \n", volpair)

						volumes = append(volumes, volpair)

						logger.Debug("Volume pairss found %v \n", volumes)

						return nil
					})
					if err != nil {
						return err
					}

				}

				masterslavestatus := api.MasterSlaveStatus{
					Id:       info.Id,
					Status:   info.Status,
					Remoteid: info.Remoteid,
					Volumes:  volumes,
					Side:     info.Side,
				}

				logger.Debug("Masterslave status is %v \n", masterslavestatus)

				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				w.WriteHeader(http.StatusOK)
				if err := json.NewEncoder(w).Encode(masterslavestatus); err != nil {
					panic(err)
				}

			}

			return nil
		})
		if err != nil {
			return
		}

	}

}
