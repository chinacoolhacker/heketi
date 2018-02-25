//
// Copyright (c) 2015 The heketi Authors
//
// This file is licensed to you under your choice of the GNU Lesser
// General Public License, version 3 or any later version (LGPLv3 or
// later), or the GNU General Public License, version 2 (GPLv2), in all
// cases as published by the Free Software Foundation.
//

package sshexec

import (
//	"fmt"

//	"github.com/lpabon/godbc"
)

func (s *SshExecutor) SshdControl(host string, action string) error {
	logger.Debug("SshdControl for host %v do %v ", host, action)
	/*
		godbc.Require(host != "")
		godbc.Require(action != "")

		// check action stop\start 2do enable\disable
		cmd := fmt.Sprintf("systemctl %v sshd ", action)

		commands := []string{cmd}
		if _, err := s.RemoteExecutor.RemoteCommandExecute(host, commands, 10); err != nil {
			return err
		}
	*/

	return nil
}
