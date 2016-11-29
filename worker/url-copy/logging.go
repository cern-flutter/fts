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
	"flag"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"gitlab.cern.ch/dmc/go-gfal2"
	"gitlab.cern.ch/flutter/fts/messages"
	"os"
	"path"
	"syscall"
	"time"
)

var logToStderr = flag.Bool("NoLog", false, "Do not redirect log to a file")
var logBaseDir = flag.String("LogDir", "/var/log/fts3/transfers", "Log directory base")
var logLevel = flag.Int("LogLevel", 0, "Log level")

// Setup the logging levels for gfal2 and underlying libraries.
func setupLogging() {
	switch *logLevel {
	default:
		fallthrough
	case 3:
		os.Setenv("CGSI_TRACE", "1")
		os.Setenv("XRD_LOGLEVEL", "Dump")
		os.Setenv("GFAL2_GRIDFTP_DEBUG", "1")
		gfal2.SetLogLevel(gfal2.LogLevelDebug)
	case 2:
		os.Setenv("CGSI_TRACE", "1")
		os.Setenv("GLOBUS_FTP_CLIENT_DEBUG_LEVEL", "255")
		os.Setenv("GLOBUS_FTP_CONTROL_DEBUG_LEVEL", "10")
		os.Setenv("GFAL2_GRIDFTP_DEBUG", "1")
		gfal2.SetLogLevel(gfal2.LogLevelDebug)
	case 1:
		gfal2.SetLogLevel(gfal2.LogLevelDebug)
		log.SetLevel(log.DebugLevel)
	case 0:
		gfal2.SetLogLevel(gfal2.LogLevelMessage)
		log.SetLevel(log.InfoLevel)
	}
}

// Generate a transfer ID from the running transfer.
func generateTransferID(transfer *messages.Transfer) string {
	return fmt.Sprintf("%s_%d", transfer.TransferId, transfer.Retry)
}

// Generate the full log path associated to a transfer.
func generateLogPath(transfer *messages.Transfer) (log string, err error) {
	transferUniqueRun := fmt.Sprintf("%s__%d", transfer.TransferId, transfer.Retry)
	dateName := time.Now().Format("2006-01-02")

	parent := path.Join(*logBaseDir, dateName, transferUniqueRun)
	err = os.MkdirAll(parent, 0755)
	log = path.Join(parent, generateTransferID(transfer))
	return
}

// Redirect logging to a file
func redirectLog(path string) {
	syscall.Close(1)
	syscall.Close(2)

	fd, err := syscall.Open(path, syscall.O_WRONLY|syscall.O_CREAT, 0664)
	if err != nil {
		log.Panic(err)
	}

	syscall.Dup2(fd, 1)
	syscall.Dup2(fd, 2)
}
