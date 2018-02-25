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
	"math"
	"net/http"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/chinacoolhacker/heketi/pkg/db"
	"github.com/chinacoolhacker/heketi/pkg/glusterfs/api"
	"github.com/chinacoolhacker/heketi/pkg/utils"
	"github.com/gorilla/mux"
	"time"
)

const (
	VOLUME_CREATE_MAX_SNAPSHOT_FACTOR = 100
)

func (a *App) VolumeCreate(w http.ResponseWriter, r *http.Request) {

	var msg api.VolumeCreateRequest
	err := utils.GetJsonFromRequest(r, &msg)
	if err != nil {
		http.Error(w, "request unable to be parsed", 422)
		return
	}
	err = msg.Validate()
	if err != nil {
		http.Error(w, "validation failed: "+err.Error(), http.StatusBadRequest)
		logger.LogError("validation failed: " + err.Error())
		return
	}

	switch {
	case msg.Gid < 0:
		http.Error(w, "Bad group id less than zero", http.StatusBadRequest)
		logger.LogError("Bad group id less than zero")
		return
	case msg.Gid >= math.MaxInt32:
		http.Error(w, "Bad group id equal or greater than 2**32", http.StatusBadRequest)
		logger.LogError("Bad group id equal or greater than 2**32")
		return
	}

	switch msg.Durability.Type {
	case api.DurabilityEC:
	case api.DurabilityReplicate:
	case api.DurabilityDistributeOnly:
	case "":
		msg.Durability.Type = api.DurabilityDistributeOnly
	default:
		http.Error(w, "Unknown durability type", http.StatusBadRequest)
		logger.LogError("Unknown durability type")
		return
	}

	if msg.Size < 1 {
		http.Error(w, "Invalid volume size", http.StatusBadRequest)
		logger.LogError("Invalid volume size")
		return
	}
	if msg.Snapshot.Enable {
		if msg.Snapshot.Factor < 1 || msg.Snapshot.Factor > VOLUME_CREATE_MAX_SNAPSHOT_FACTOR {
			http.Error(w, "Invalid snapshot factor", http.StatusBadRequest)
			logger.LogError("Invalid snapshot factor")
			return
		}
	}

	if msg.Durability.Type == api.DurabilityReplicate {
		if msg.Durability.Replicate.Replica > 3 {
			http.Error(w, "Invalid replica value", http.StatusBadRequest)
			logger.LogError("Invalid replica value")
			return
		}
	}

	if msg.Durability.Type == api.DurabilityEC {
		d := msg.Durability.Disperse
		// Place here correct combinations
		switch {
		case d.Data == 2 && d.Redundancy == 1:
		case d.Data == 4 && d.Redundancy == 2:
		case d.Data == 8 && d.Redundancy == 3:
		case d.Data == 8 && d.Redundancy == 4:
		default:
			http.Error(w,
				fmt.Sprintf("Invalid dispersion combination: %v+%v", d.Data, d.Redundancy),
				http.StatusBadRequest)
			logger.LogError(fmt.Sprintf("Invalid dispersion combination: %v+%v", d.Data, d.Redundancy))
			return
		}
	}

	// Check that the clusters requested are available
	err = a.db.View(func(tx *bolt.Tx) error {

		// :TODO: All we need to do is check for one instead of gathering all keys
		clusters, err := ClusterList(tx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}
		if len(clusters) == 0 {
			http.Error(w, fmt.Sprintf("No clusters configured"), http.StatusBadRequest)
			logger.LogError("No clusters configured")
			return ErrNotFound
		}
		// check if no defined clusters, select one of master or slave if they defined
		// Check the clusters requested are correct
		for _, clusterid := range msg.Clusters {
			_, err := NewClusterEntryFromId(tx, clusterid)
			if err != nil {
				http.Error(w, fmt.Sprintf("Cluster id %v not found", clusterid), http.StatusBadRequest)
				logger.LogError(fmt.Sprintf("Cluster id %v not found", clusterid))
				return err
			}
		}

		return nil
	})
	if err != nil {
		return
	}
	// if no clusters defined then get master and slave, and create 2 volumes
	// with geo-replication on it
	// todo: support 2+ clusters, now only 2 supported
	MasterCluster := []string{}
	SlaveCluster := []string{}

	if len(msg.Clusters) == 0 {
		MasterCluster, SlaveCluster = a.MasterSlaveClustersCheck()
		logger.Debug("Master Cluster found as %v and Slave Cluster found as %v \n", MasterCluster, SlaveCluster)
	}

	vol := NewVolumeEntryFromRequest(&msg)
	// todo set only if no masters found etc. non crit.
	remvol := NewVolumeEntryFromRequest(&msg)

	// Masterslave preparations for volume if master exists
	if len(MasterCluster) != 0 {
		vol.Info.Remvolid = remvol.Info.Id
		vol.Info.Clusters = MasterCluster
		remvol.Info.Remvolid = vol.Info.Id
		remvol.Info.Clusters = SlaveCluster

		logger.Debug("For volume %v set clusters %v and for remote volume %v set clusters %v \n", vol.Info.Id, vol.Info.Clusters, remvol.Info.Id, remvol.Info.Clusters)

	}

	if uint64(msg.Size)*GB < vol.Durability.MinVolumeSize() {
		http.Error(w, fmt.Sprintf("Requested volume size (%v GB) is "+
			"smaller than the minimum supported volume size (%v)",
			msg.Size, vol.Durability.MinVolumeSize()),
			http.StatusBadRequest)
		logger.LogError(fmt.Sprintf("Requested volume size (%v GB) is "+
			"smaller than the minimum supported volume size (%v)",
			msg.Size, vol.Durability.MinVolumeSize()))
		return
	}

	vc := NewVolumeCreateOperation(vol, a.db)
	if err := AsyncHttpOperation(a, w, r, vc); err != nil {
		http.Error(w,
			fmt.Sprintf("Failed to allocate new volume: %v", err),
			http.StatusInternalServerError)
		return
	}

	// masterslave go on
	if len(MasterCluster) != 0 {
		masterSshCluster := strings.Join(MasterCluster, ",")

		if uint64(msg.Size)*GB < remvol.Durability.MinVolumeSize() {
			http.Error(w, fmt.Sprintf("Requested volume size (%v GB) is "+
				"smaller than the minimum supported volume size (%v)",
				msg.Size, remvol.Durability.MinVolumeSize()),
				http.StatusBadRequest)
			logger.LogError(fmt.Sprintf("Requested volume size (%v GB) is "+
				"smaller than the minimum supported volume size (%v)",
				msg.Size, remvol.Durability.MinVolumeSize()))
			return
		}

		remvc := NewVolumeCreateOperation(remvol, a.db)

		if err := AsyncHttpOperation(a, w, r, remvc); err != nil {
			http.Error(w,
				fmt.Sprintf("Failed to allocate new replicated volume: %v", err),
				http.StatusInternalServerError)
			return
		}

		logger.Debug("For Vol %v Selected host %v from hosts %v", vol.Info.Id, vol.Info.Mount.GlusterFS.Hosts[0], vol.Info.Mount.GlusterFS.Hosts)
		logger.Debug("For Vol %v Selected host %v from hosts %v", remvol.Info.Id, remvol.Info.Mount.GlusterFS.Hosts[0], remvol.Info.Mount.GlusterFS.Hosts)

		// Create Slave-master geo session without start for switdhower needs
		a.asyncManager.AsyncHttpRedirectFunc(w, r, func() (string, error) {
			time.Sleep(60 * time.Second)

			// start sshd on master to init georep session
			sshonerr := a.MasterSlaveSshdSet("start", masterSshCluster)
			if sshonerr != nil {
				logger.LogError("Error during stop ssh : %v \n", sshonerr)
			}

			//todo: wait for volume create

			actionParams := make(map[string]string)
			actionParams["option"] = "push-pem"
			actionParams["force"] = "true"

			geoRepCreateRequest := api.GeoReplicationRequest{
				Action:       api.GeoReplicationActionCreate,
				ActionParams: actionParams,
				GeoReplicationInfo: api.GeoReplicationInfo{
					SlaveHost:    vol.Info.Mount.GlusterFS.Hosts[0],
					SlaveVolume:  vol.Info.Name,
					SlaveSSHPort: 2222,
				},
			}

			id := remvol.Info.Id
			var masterVolume *VolumeEntry
			var host string

			err = a.db.View(func(tx *bolt.Tx) error {
				masterVolume, err = NewVolumeEntryFromId(tx, id)
				logger.Debug("For volume geo %v with id %v geo \n", masterVolume, masterVolume.Info.Id)

				if err == ErrNotFound {
					logger.LogError("[ERROR] Volume Id not found: %v \n", err)
					return err
				} else if err != nil {
					logger.LogError("[ERROR] Internal error: %v \n", err)
					return err
				}

				cluster, err := NewClusterEntryFromId(tx, masterVolume.Info.Cluster)
				if err == ErrNotFound {
					return err
				} else if err != nil {
					return err
				}

				node, err := NewNodeEntryFromId(tx, cluster.Info.Nodes[0])
				if err == ErrNotFound {
					logger.LogError("[ERROR] Node Id not found: %v", err)
					return err
				} else if err != nil {
					logger.LogError("[ERROR] Internal error: %v", err)
					return err
				}

				host = node.ManageHostName()

				return nil
			})

			if err != nil {
				logger.LogError("Error during found master volume : %v \n", err)
				return "", err
			}

			logger.Debug("Create geo replicate with request %v \n", geoRepCreateRequest)
			if err := masterVolume.GeoReplicationAction(a.db, a.executor, host, geoRepCreateRequest); err != nil {
				return "", err
			}

			return "/volumes/" + masterVolume.Info.Id + "/georeplication", nil
		})

		// Creater master-slave session
		// Perform GeoReplication action on volume in an asynchronous function
		a.asyncManager.AsyncHttpRedirectFunc(w, r, func() (string, error) {
			time.Sleep(60 * time.Second)

			actionParams := make(map[string]string)
			actionParams["option"] = "push-pem"
			actionParams["force"] = "true"

			geoRepCreateRequest := api.GeoReplicationRequest{
				Action:       api.GeoReplicationActionCreate,
				ActionParams: actionParams,
				GeoReplicationInfo: api.GeoReplicationInfo{
					SlaveHost:    remvol.Info.Mount.GlusterFS.Hosts[0],
					SlaveVolume:  remvol.Info.Name,
					SlaveSSHPort: 2222,
				},
			}

			id := vol.Info.Id
			var masterVolume *VolumeEntry
			var host string

			err = a.db.View(func(tx *bolt.Tx) error {
				masterVolume, err = NewVolumeEntryFromId(tx, id)
				logger.Debug("For volume geo %v with id %v geo \n", masterVolume, masterVolume.Info.Id)

				if err == ErrNotFound {
					logger.LogError("[ERROR] Volume Id not found: %v \n", err)
					http.Error(w, "Volume Id not found", http.StatusNotFound)
					return err
				} else if err != nil {
					logger.LogError("[ERROR] Internal error: %v \n", err)
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return err
				}

				cluster, err := NewClusterEntryFromId(tx, masterVolume.Info.Cluster)
				if err == ErrNotFound {
					http.Error(w, "Cluster Id not found", http.StatusNotFound)
					return err
				} else if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return err
				}

				node, err := NewNodeEntryFromId(tx, cluster.Info.Nodes[0])
				if err == ErrNotFound {
					logger.LogError("[ERROR] Node Id not found: %v", err)
					http.Error(w, "Node Id not found", http.StatusNotFound)
					return err
				} else if err != nil {
					logger.LogError("[ERROR] Internal error: %v", err)
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return err
				}

				host = node.ManageHostName()

				return nil
			})

			if err != nil {
				logger.LogError("[ERROR] Error during found master volume : %v \n", err)
				return "", err
			}

			logger.Debug("Create geo replicate with request %v \n", geoRepCreateRequest)
			if err := masterVolume.GeoReplicationAction(a.db, a.executor, host, geoRepCreateRequest); err != nil {
				return "", err
			}

			geoRepStartRequest := api.GeoReplicationRequest{
				Action: api.GeoReplicationActionStart,
				GeoReplicationInfo: api.GeoReplicationInfo{
					SlaveHost:   remvol.Info.Mount.GlusterFS.Hosts[0],
					SlaveVolume: remvol.Info.Name,
				},
			}

			//todo: should be performed after volume create
			logger.Debug("Start geo replicate with request %v \n", geoRepStartRequest)

			if err := masterVolume.GeoReplicationAction(a.db, a.executor, host, geoRepStartRequest); err != nil {
				return "", err
			}

			logger.Info("Geo-Replication is started for volume: %v \n", masterVolume)

			// 2do : check if vol created
			time.Sleep(30 * time.Second)
			// disable sshd on master
			sshofferr := a.MasterSlaveSshdSet("stop", masterSshCluster)
			if sshofferr != nil {
				logger.LogError("Error during stop ssh : %v \n", sshofferr)
			}

			return "/volumes/" + masterVolume.Info.Id + "/georeplication", nil

		})

	}

}

func (a *App) VolumeList(w http.ResponseWriter, r *http.Request) {

	var list api.VolumeListResponse

	// Get all the cluster ids from the DB
	err := a.db.View(func(tx *bolt.Tx) error {
		var err error

		list.Volumes, err = ListCompleteVolumes(tx)
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
	// Send list back
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(list); err != nil {
		panic(err)
	}
}

func (a *App) VolumeInfo(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var info *api.VolumeInfoResponse
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

		info, err = entry.NewInfoResponse(tx)
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
	if err := json.NewEncoder(w).Encode(info); err != nil {
		panic(err)
	}

}

func (a *App) VolumeDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var remotevolumeid string
	MasterCluster := []string{}
	MasterCluster, _ = a.MasterSlaveClustersCheck()

	var volume *VolumeEntry
	err := a.db.View(func(tx *bolt.Tx) error {

		var err error
		volume, err = NewVolumeEntryFromId(tx, id)

		if len(MasterCluster) != 0 {
			remotevolumeid = volume.Info.Remvolid
			logger.Debug("Remote Volume with id %v found \n", remotevolumeid)
		}

		if err == ErrNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return err
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}

		if volume.Info.Name == db.HeketiStorageVolumeName {
			err := fmt.Errorf("Cannot delete volume containing the Heketi database")
			http.Error(w, err.Error(), http.StatusConflict)
			return err
		}

		if !volume.Info.Block {
			// further checks only needed for block-hosting volumes
			return nil
		}

		if volume.Info.BlockInfo.BlockVolumes == nil {
			return nil
		}

		if len(volume.Info.BlockInfo.BlockVolumes) == 0 {
			return nil
		}

		err = logger.LogError("Cannot delete a block hosting volume containing block volumes")
		http.Error(w, err.Error(), http.StatusConflict)
		return err

	})
	if err != nil {
		return
	}

	vdel := NewVolumeDeleteOperation(volume, a.db)
	if err := AsyncHttpOperation(a, w, r, vdel); err != nil {
		http.Error(w,
			fmt.Sprintf("Failed to set up volume delete: %v", err),
			http.StatusInternalServerError)
		return
	}

	if remotevolumeid != "" {
		logger.Debug("For remote Volume id %v \n", remotevolumeid)
		var volume *VolumeEntry
		err := a.db.View(func(tx *bolt.Tx) error {

			var err error
			volume, err = NewVolumeEntryFromId(tx, remotevolumeid)

			if err == ErrNotFound {
				http.Error(w, err.Error(), http.StatusNotFound)
				return err
			} else if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return err
			}

			if volume.Info.Name == db.HeketiStorageVolumeName {
				err := fmt.Errorf("Cannot delete volume containing the Heketi database")
				http.Error(w, err.Error(), http.StatusConflict)
				return err
			}

			if !volume.Info.Block {
				// further checks only needed for block-hosting volumes
				return nil
			}

			if volume.Info.BlockInfo.BlockVolumes == nil {
				return nil
			}

			if len(volume.Info.BlockInfo.BlockVolumes) == 0 {
				return nil
			}

			err = logger.LogError("Cannot delete a block hosting volume containing block volumes")
			http.Error(w, err.Error(), http.StatusConflict)
			return err

		})
		if err != nil {
			return
		}

		vdel := NewVolumeDeleteOperation(volume, a.db)
		if err := AsyncHttpOperation(a, w, r, vdel); err != nil {
			http.Error(w,
				fmt.Sprintf("Failed to set up volume delete: %v", err),
				http.StatusInternalServerError)
			return
		}
	}

}

func (a *App) VolumeExpand(w http.ResponseWriter, r *http.Request) {
	logger.Debug("In VolumeExpand")

	vars := mux.Vars(r)
	id := vars["id"]

	var msg api.VolumeExpandRequest
	err := utils.GetJsonFromRequest(r, &msg)
	if err != nil {
		http.Error(w, "request unable to be parsed", 422)
		return
	}
	logger.Debug("Msg: %v", msg)
	err = msg.Validate()
	if err != nil {
		http.Error(w, "validation failed: "+err.Error(), http.StatusBadRequest)
		logger.LogError("validation failed: " + err.Error())
		return
	}

	if msg.Size < 1 {
		http.Error(w, "Invalid volume size", http.StatusBadRequest)
		return
	}
	logger.Debug("Size: %v", msg.Size)

	var volume *VolumeEntry
	err = a.db.View(func(tx *bolt.Tx) error {

		var err error
		volume, err = NewVolumeEntryFromId(tx, id)
		if err == ErrNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return err
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}

		return nil

	})
	if err != nil {
		return
	}

	ve := NewVolumeExpandOperation(volume, a.db, msg.Size)
	if err := AsyncHttpOperation(a, w, r, ve); err != nil {
		http.Error(w,
			fmt.Sprintf("Failed to allocate volume expansion: %v", err),
			http.StatusInternalServerError)
		return
	}
}
