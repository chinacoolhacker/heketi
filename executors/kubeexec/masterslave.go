//
// Copyright (c) 2015 The heketi Authors
//
// This file is licensed to you under your choice of the GNU Lesser
// General Public License, version 3 or any later version (LGPLv3 or
// later), or the GNU General Public License, version 2 (GPLv2), in all
// cases as published by the Free Software Foundation.
//

package kubeexec

import (
	"fmt"

	"github.com/lpabon/godbc"
)

func (s *KubeExecutor) SshdControl(host string, action string) error {
	logger.Debug("SshdControl for host %v do %v ", host, action)
	var subaction string

	godbc.Require(host != "")
	godbc.Require(action != "")

	switch {
	case action == "start":
		subaction = "enable"
	case action == "stop":
		subaction = "disable"
	default:
		logger.LogError("Invalid action")
	}

	// check action stop\start 2do enable\disable
	cmd := fmt.Sprintf("systemctl %v sshd && systemctl %v sshd ", action, subaction)

	commands := []string{cmd}
	if _, err := s.RemoteExecutor.RemoteCommandExecute(host, commands, 10); err != nil {
		return err
	}

	return nil
}
