/*
 * Copyright (c) CERN 2016
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/satori/go.uuid"
	"gitlab.cern.ch/flutter/fts/messages"
	"os"
	"os/exec"
	"path"
	"strings"
	"syscall"
)

func writeProxy(proxy *X509Proxy, path string) error {
	var err error
	var fd *os.File
	if fd, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0600); err != nil {
		return fmt.Errorf("Could not create the proxy file: %s", err.Error())
	}
	defer fd.Close()

	if _, err = fd.Write(proxy.Proxy); err != nil {
		return fmt.Errorf("Could not write the proxy file: %s", err.Error())
	}
	return nil
}

func writeTaskSet(task *messages.Batch, path string) error {
	var err error
	var fd *os.File

	if fd, err = os.Create(path); err != nil {
		return fmt.Errorf("Could not create task file: %s", err.Error())
	}
	defer fd.Close()

	var data []byte
	if data, err = json.Marshal(task); err != nil {
		os.Remove(path)
		return fmt.Errorf("Failed to serialize the task: %s", err.Error())
	}

	if _, err = fd.Write(data); err != nil {
		os.Remove(path)
		return fmt.Errorf("Failed to write the serialized task: %s", err.Error())
	}
	fd.Close()
	return nil
}

// RunTransfer spawns a new url-copy, and returns its pid on success
func RunTransfer(c *Worker, task *messages.Batch) (int, error) {
	var proxy X509Proxy
	var err error
	// TODO: Get proxy

	taskFile := path.Join("/tmp/", uuid.NewV1().String())
	pemFile := path.Join("/tmp/", fmt.Sprintf("x509up_h%s", task.CredId))

	if err = writeProxy(&proxy, pemFile); err != nil {
		return 0, err
	}
	if err = writeTaskSet(task, taskFile); err != nil {
		return 0, err
	}

	cmd := exec.Command(c.params.URLCopyBin,
		"-LogLevel", fmt.Sprintf("%d", log.GetLevel()),
		"-DirQ", c.params.DirQPath,
		"-LogDir", c.params.TransferLogPath,
		"-Proxy", pemFile,
		"-KeepTaskFile",
		taskFile,
	)
	cmd.Dir = "/tmp"
	cmd.Stdin = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	log.Debug("Spawning ", strings.Join(cmd.Args, " "))
	if err = cmd.Start(); err != nil {
		os.Remove(taskFile)
		return 0, fmt.Errorf("Failed to run the command: %s", err.Error())
	}
	return cmd.Process.Pid, nil
}
