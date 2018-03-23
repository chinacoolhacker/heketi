package client

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/chinacoolhacker/heketi/pkg/glusterfs/api"
	"github.com/heketi/utils"
)

func (c *Client) MasterClusterSlavePostAction(id string, request *api.ClusterSetMasterSlaveRequest) error {
	// Marshal request to JSON
	buffer, err := json.Marshal(request)
	if err != nil {
		return  err
	}

	// Create a request
	req, err := http.NewRequest("POST", c.host+"/clusters/"+id+"/masterslave", bytes.NewBuffer(buffer))
	if err != nil {
		return  err
	}
	req.Header.Set("Content-Type", "application/json")

	// Set token
	err = c.setToken(req)
	if err != nil {
		return  err
	}

	// Send request
	r, err := c.do(req)
	if err != nil {
		return  err
	}
	if r.StatusCode != http.StatusAccepted {
		return  utils.GetErrorFromResponse(r)
	}

	// Wait for response
	r, err = c.waitForResponseWithTimer(r, time.Second)
	if err != nil {
		return err
	}
	if r.StatusCode != http.StatusOK {
		return utils.GetErrorFromResponse(r)
	}

	return nil
}

