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
	"github.com/gorilla/mux"
	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/heketi/utils"
	"net/http"
	"reflect"
)

// MasterSlaveStatus of cluster
// undone
func (a *App) MasterSlaveClustersStatus(w http.ResponseWriter, r *http.Request) {
	logger.Debug("In MasterSlaveClustersStatus")
	MasterClusters, SlaveClusters := a.MasterSlaveClustersCheck()

	fmt.Printf("CHECKKKKKKKK in MasterCluster  %v \n", MasterClusters)
	fmt.Printf("CHECKKKKKKKK in SlaveCluster  %v \n", SlaveClusters)

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
			fmt.Printf("No MasterSlave configured yet %v\n", info.Remoteid)
			return err
		}
		if err != nil {
			return err
		}
		err = UpdateClusterInfoComplete(tx, info)
		if err != nil {
			return err
		}
		fmt.Printf("REMID %v\n", info.Remoteid)
		fmt.Printf("Status %v\n", info.Status)
		fmt.Printf("VOLS %v\n", info.Volumes)

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
		//		fmt.Printf("tx %v \n ", tx)
		//		fmt.Printf("rement  %v \n ", rementry)
		//		fmt.Printf("remid  %v \n ", rementry.Info.Remoteid)
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

		switch msg.Status {
		case "master":
			entry.Info.Status = msg.Status
			rementry.Info.Status = "slave"
			//
			//rementry.MasterSlaveEnslave
			//entry.MasterSlaveEnmaster
		case "slave":
			entry.Info.Status = msg.Status
			rementry.Info.Status = "master"
			//
			//entry.MasterSlaveEnslave
			//rementry.MasterSlaveEnmaster
		case "":

		default:
			//////
			fmt.Printf("Status %v invalid - use master or slave \n ", msg.Status)
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
// undone
func (a *App) MasterSlaveStatus(w http.ResponseWriter, r *http.Request) {

	var clusters []string

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
	// dbg
	fmt.Printf("%v\n", clusters)
	fmt.Println(reflect.TypeOf(clusters))

	for _, id := range clusters {

		var info *api.ClusterInfoResponse
		//		var info []string
		err := a.db.View(func(tx *bolt.Tx) error {

			// Create a db entry from the id
			entry, err := NewClusterEntryFromId(tx, id)
			if err == ErrNotFound {
				http.Error(w, err.Error(), http.StatusNotFound)
				return err
			} else if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return err
			}
			//dbg
			fmt.Printf("%v\n", entry)
			fmt.Println(reflect.TypeOf(entry))

			//			fmt.Printf("%v\n", tx)
			//			fmt.Println(reflect.TypeOf(tx))

			// Create a response from the db entry
			info, err = entry.NewClusterInfoResponse(tx)
			if err != nil {
				return err
			}
			err = UpdateClusterInfoComplete(tx, info)
			if err != nil {
				return err
			}

			// dbg
			//			fmt.Printf("%v\n", info)
			//			fmt.Println(reflect.TypeOf(info))

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

		/////////////////////////////////////////////////////////////////////////////
	}

	/*
		// Send clusters back
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(clusters); err != nil {
			panic(err)
		}
	*/
}
