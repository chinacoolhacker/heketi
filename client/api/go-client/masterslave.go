package client

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/chinacoolhacker/heketi/pkg/glusterfs/api"
	"github.com/heketi/utils"
)

func (c *Client) MasterClusterSlavePostAction(id string, request *api.ClusterSetMasterSlaveRequest) error {
	// Marshal request to JSON
	buffer, err := json.Marshal(request)
	if err != nil {
		return err
	}

	// Create a request
	req, err := http.NewRequest("POST", c.host+"/clusters/"+id+"/masterslave", bytes.NewBuffer(buffer))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	// Set token
	err = c.setToken(req)
	if err != nil {
		return err
	}

	// Send request
	r, err := c.do(req)
	if err != nil {
		return err
	}

	if r.StatusCode != http.StatusOK {
		return utils.GetErrorFromResponse(r)
	}

	return nil
}
func (c *Client) MasterSlaveClusterStatus(id string) (*api.MasterSlaveClusterStatus, error) {
	// Create request
	req, err := http.NewRequest("GET", c.host+"/clusters/"+id+"/masterslave", nil)
	if err != nil {
		return nil, err
	}

	// Set token
	err = c.setToken(req)
	if err != nil {
		return nil, err
	}

	// Get status
	r, err := c.do(req)
	if err != nil {
		return nil, err
	}

	if r.StatusCode != http.StatusOK {
		return nil, utils.GetErrorFromResponse(r)
	}

	// Read JSON response
	var status api.MasterSlaveClusterStatus
	err = utils.GetJsonFromResponse(r, &status)
	r.Body.Close()
	if err != nil {
		return nil, err
	}

	return &status, nil
}

func (c *Client) MasterSlaveStatus() (*api.MasterSlaveStatus, error) {
	// Create request
	req, err := http.NewRequest("GET", c.host+"/masterslave", nil)
	if err != nil {
		return nil, err
	}

	// Set token
	err = c.setToken(req)
	if err != nil {
		return nil, err
	}

	// Get status
	r, err := c.do(req)
	if err != nil {
		return nil, err
	}
	if r.StatusCode != http.StatusOK {
		return nil, utils.GetErrorFromResponse(r)
	}

	// Read JSON response
	var status api.MasterSlaveStatus
	err = utils.GetJsonFromResponse(r, &status)
	r.Body.Close()
	if err != nil {
		return nil, err
	}

	return &status, nil
}
