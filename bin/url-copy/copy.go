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
	"flag"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"gitlab.cern.ch/dmc/go-gfal2"
	"gitlab.cern.ch/flutter/fts/types/perf"
	"gitlab.cern.ch/flutter/fts/types/tasks"
	"gitlab.cern.ch/flutter/fts/version"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
)

var keepTaskFile = flag.Bool("KeepTaskFile", false, "Do not delete the task file once read")
var x509proxy = flag.String("Proxy", "", "User X509 proxy")

type urlCopy struct {
	context  *gfal2.Context
	canceled bool
	failures int

	mutex         sync.Mutex
	batch         tasks.Batch
	transferIndex int
	terminalSent  bool

	// Transfer being run
	transfer *tasks.Transfer
}

// NewUrlCopy creates a new instance of UrlCopy, initialized using the task
// serialized in the file pointed by taskfile.
func newURLCopy(taskfile string) *urlCopy {
	var err error
	copy := urlCopy{}

	raw, err := ioutil.ReadFile(taskfile)
	if err != nil {
		log.Panic("Could not open the task file: ", err.Error())
	}
	err = json.Unmarshal(raw, &copy.batch)
	if err != nil {
		log.Panic("Failed to parse the task file: ", err.Error())
	}

	if !*keepTaskFile {
		os.Remove(taskfile)
	}

	// All transfers, from now on, will have a status
	for index := range copy.batch.Transfers {
		copy.batch.Transfers[index].Info = &tasks.TransferInfo{}
	}

	copy.context, err = gfal2.NewContext()
	if err != nil {
		copy.Panic("Could not instantiate the gfal2 context: %s", err.Error())
	}
	return &copy
}

// next returns the next transfer in the list, nil on end
func (copy *urlCopy) next() *tasks.Transfer {
	copy.mutex.Lock()
	defer copy.mutex.Unlock()

	if copy.transferIndex < len(copy.batch.Transfers) {
		transfer := copy.batch.Transfers[copy.transferIndex]
		copy.transferIndex++
		return transfer
	}
	return nil
}

// setupGfal2ForTransfer prepares the gfal2 context for this particular transfer.
func (copy *urlCopy) setupGfal2ForTransfer(transfer *tasks.Transfer) {
	copy.context.SetOptBoolean("GRIDFTP PLUGIN", "SESSION_REUSE", true)
	copy.context.SetOptBoolean("GRIDFTP PLUGIN", "ENABLE_UDT", transfer.Parameters.EnableUdt)
	copy.context.SetOptBoolean("GRIDFTP PLUGIN", "IPV6", transfer.Parameters.EnableIpv6)
	copy.context.SetUserAgent("url_copy", version.Version)
	copy.context.SetOptString("X509", "CERT", fmt.Sprintf("/tmp/proxy_%s.pem", copy.batch.DelegationID))
	copy.context.SetOptString("X509", "KEY", fmt.Sprintf("/tmp/proxy_%s.pem", copy.batch.DelegationID))

	switch transfer.Parameters.ChecksumMode {
	case tasks.ChecksumRelaxed:
		copy.context.SetOptBoolean("SRM PLUGIN", "ALLOW_EMPTY_SOURCE_CHECKSUM", true)
		copy.context.SetOptBoolean("GRIDFTP PLUGIN", "SKIP_SOURCE_CHECKSUM", true)
		copy.context.SetOptString("XROOTD PLUGIN", "COPY_CHECKSUM_MODE", "target")
	case tasks.ChecksumStrict:
		copy.context.SetOptBoolean("SRM PLUGIN", "ALLOW_EMPTY_SOURCE_CHECKSUM", false)
		copy.context.SetOptBoolean("GRIDFTP PLUGIN", "SKIP_SOURCE_CHECKSUM", false)
		copy.context.SetOptString("XROOTD PLUGIN", "COPY_CHECKSUM_MODE", "end2end")
	}

	if *x509proxy != "" {
		copy.context.SetOptString("X509", "CERT", *x509proxy)
		copy.context.SetOptString("X509", "KEY", *x509proxy)
	}
}

// createCopyHandler creates and prepares a gfal2 copy handler to run the transfer.
func (copy *urlCopy) createCopyHandler(transfer *tasks.Transfer) (handler *gfal2.TransferHandler, err gfal2.GError) {
	handler, err = copy.context.NewTransferHandler()
	if err != nil {
		return
	}

	handler.SetCreateParentDir(true)
	handler.SetStrictCopy(transfer.Parameters.OnlyCopy)
	handler.SetOverwrite(transfer.Parameters.Overwrite)
	if transfer.Parameters.SourceSpaceToken != nil {
		handler.SetSourceSpacetoken(*transfer.Parameters.SourceSpaceToken)
	}
	if transfer.Parameters.DestSpaceToken != nil {
		handler.SetDestinationSpaceToken(*transfer.Parameters.DestSpaceToken)
	}

	if transfer.Parameters.ChecksumMode != tasks.ChecksumSkip {
		handler.EnableChecksum(true)
	}
	if transfer.Checksum != nil {
		parts := strings.SplitN(string(*transfer.Checksum), ":", 2)
		if len(parts) == 2 {
			handler.SetChecksum(parts[0], parts[1])
		} else {
			log.Warning("Invalid checksum passed. Ignoring.")
		}
	}

	handler.AddMonitorCallback(copy)

	return
}

// Called by gfal2 when there are performance markers available.
func (copy *urlCopy) NotifyPerformanceMarker(marker gfal2.Marker) {
	perf := &perf.Marker{
		JobID:              copy.transfer.JobID,
		TransferID:         copy.transfer.TransferID,
		SourceStorage:      copy.transfer.Source.GetStorageName(),
		DestinationStorage: copy.transfer.Destination.GetStorageName(),
		Throughput:         marker.AvgThroughput,
		TransferredBytes:   marker.BytesTransferred,
	}
	if err := copy.reportPerformance(perf); err != nil {
		log.Error(err)
	}
	copy.transfer.Info.Stats.TransferredBytes = int64(marker.BytesTransferred)
	copy.transfer.Info.Stats.Throughput = float32(marker.AvgThroughput)
}

// runTransfer prepares the context and copy handler, and run the transfer. It sends the start message,
// and returns the termination message, but it does not send it.
func (copy *urlCopy) runTransfer(transfer *tasks.Transfer) {
	var err error

	transfer.Info = &tasks.TransferInfo{
		Error:   nil,
		Message: "Starting transfer",
		Stats:   &tasks.TransferRunStatistics{},
		LogFile: nil,
	}
	transfer.Info.Stats.Intervals.Total.Start = time.Now()

	logFile, err := generateLogPath(transfer)
	if err != nil {
		copy.Panic("Could not create the log file: %s", err.Error())
	}
	if !*logToStderr {
		redirectLog(logFile)
		transfer.Info.LogFile = &logFile
	}

	log.Info("Transfer accepted")
	if err != nil {
		log.Errorf("Failed to send the start message: %s", err.Error())
	}

	copy.setupGfal2ForTransfer(transfer)
	params, gerr := copy.createCopyHandler(transfer)
	if gerr != nil {
		transfer.Info.Error = &tasks.TransferError{
			Scope:       tasks.ScopeAgent,
			Code:        gerr.Code(),
			Description: gerr.Error(),
			Recoverable: false,
		}
		return
	}

	log.Infof("Proxy: %s", copy.context.GetOptString("X509", "CERT"))
	log.Infof("Job id: %s", transfer.JobID)
	log.Infof("Transfer id: %s", transfer.TransferID)
	log.Infof("Source url: %s", transfer.Source.String())
	log.Infof("Dest url: %s", transfer.Destination.String())
	log.Infof("Overwrite enabled: %t", params.GetOverwrite())

	if transfer.Parameters.DestSpaceToken != nil {
		log.Infof("Dest space token: %s", *transfer.Parameters.DestSpaceToken)
	}
	if transfer.Parameters.SourceSpaceToken != nil {
		log.Infof("Source space token: %s", *transfer.Parameters.SourceSpaceToken)
	}
	if transfer.Checksum != nil {
		log.Infof("Checksum: %s", *transfer.Checksum)
	}

	if transfer.Filesize != nil {
		log.Infof("User filesize: %d", *transfer.Filesize)
	}

	log.Infof("Multihop: %t", copy.batch.Type == tasks.BatchMultihop)
	log.Infof("IPv6: %t", copy.context.GetOptBoolean("GRIDFTP PLUGIN", "IPV6"))
	log.Infof("UDT: %t", copy.context.GetOptBoolean("GRIDFTP PLUGIN", "ENABLE_UDT"))
	if copy.context.GetOptBoolean("BDII", "ENABLED") {
		log.Infof("BDII: %s", copy.context.GetOptString("BDII", "LCG_GFAL_INFOSYS"))
	}

	srcStat, gerr := copy.context.Stat(transfer.Source.String())
	if gerr != nil {
		transfer.Info.Error = &tasks.TransferError{
			Scope:       tasks.ScopeSource,
			Code:        gerr.Code(),
			Description: gerr.Error(),
			Recoverable: IsRecoverable(tasks.ScopeSource, gerr.Code()),
		}
		return
	}

	log.Infof("File size: %d", srcStat.Size())

	if transfer.Filesize != nil && *transfer.Filesize != 0 && *transfer.Filesize != srcStat.Size() {
		transfer.Info.Error = &tasks.TransferError{
			Scope: tasks.ScopeSource,
			Code:  syscall.EINVAL,
			Description: fmt.Sprintf(
				"Source and user provide file sizes do not match: %d != %d",
				srcStat.Size(), *transfer.Filesize,
			),
			Recoverable: false,
		}
		return
	} else if transfer.Filesize == nil {
		transfer.Filesize = new(int64)
		*transfer.Filesize = srcStat.Size()
	}

	var timeout time.Duration
	if transfer.Parameters.Timeout != 0 {
		timeout = time.Duration(transfer.Parameters.Timeout * int(time.Second))
	} else {
		timeout = AdjustTimeoutBasedOnSize(srcStat.Size()) + (600 * time.Second)
	}
	log.Infof("Timeout: %.0f seconds", timeout.Seconds())

	// Run the copy in a separate goroutine so we can catch timeouts
	done := make(chan bool, 1)
	go func() {
		gerr = params.CopyFile(transfer.Source.String(), transfer.Destination.String())
		done <- true
	}()

	select {
	case _ = <-done:
	case <-time.After(timeout):
		copy.context.Cancel()
		transfer.Info.Error = &tasks.TransferError{
			Scope:       tasks.ScopeTransfer,
			Code:        syscall.ETIMEDOUT,
			Description: "Transfer timed out",
			Recoverable: true,
		}
		return
	}

	transfer.Info.Stats.Intervals.Total.End = time.Now()

	if gerr != nil {
		transfer.Info.Error = &tasks.TransferError{
			Scope:       tasks.ScopeTransfer,
			Code:        gerr.Code(),
			Description: gerr.Error(),
			Recoverable: IsRecoverable(tasks.ScopeTransfer, gerr.Code()),
		}
	} else {
		// Adjust size and throughput if the protocol didn't give us perf.markers
		if transfer.Info.Stats.TransferredBytes == 0 {
			transfer.Info.Stats.TransferredBytes = srcStat.Size()
		}
		if transfer.Info.Stats.Throughput == 0 {
			transfer.Info.Stats.Throughput = float32(
				float64(srcStat.Size()) / (transfer.Info.Stats.Intervals.Total.Seconds()),
			)
		}
	}
	return
}

// sendTerminalForRemaining send a terminal message for any transfer than hasn't run yet.
// This could be due to external cancellation, or multihop failures.
func (copy *urlCopy) setStateForRemaining() {
	var remainingInfo *tasks.TransferInfo
	var remainingState tasks.TransferState

	switch copy.batch.Type {
	// For multihop, one failure = shortcut to all remaining failed
	case tasks.BatchMultihop:
		if copy.failures > 0 {
			remainingState = tasks.TransferFailed
			remainingInfo = &tasks.TransferInfo{
				Error: &tasks.TransferError{
					Scope:       tasks.ScopeTransfer,
					Code:        syscall.ECANCELED,
					Description: "Transfer canceled because a previous hop failed",
					Recoverable: false,
				},
				Stats:   nil,
				LogFile: nil,
			}
		} else {
			remainingState = tasks.TransferOnHold
			remainingInfo = &tasks.TransferInfo{
				Stats:   nil,
				LogFile: nil,
			}
		}
	// For multisources, one success = shortcut to all remaining to unused
	case tasks.BatchMultisource:
		if copy.failures == 0 {
			remainingState = tasks.TransferUnused
			remainingInfo = &tasks.TransferInfo{
				Stats:   nil,
				LogFile: nil,
			}
		} else {
			remainingState = tasks.TransferOnHold
			remainingInfo = &tasks.TransferInfo{
				Stats:   nil,
				LogFile: nil,
			}
		}
	// For the rest, do nothing
	default:
		return
	}

	for transfer := copy.next(); transfer != nil; transfer = copy.next() {
		transfer.State = remainingState
		transfer.Info = remainingInfo
	}
}

// Run runs the whole process.
func (copy *urlCopy) Run() {
	copy.reportBatchStart()
	for copy.transfer = copy.next(); copy.transfer != nil && !copy.canceled; copy.transfer = copy.next() {
		copy.runTransfer(copy.transfer)

		if copy.transfer.Info.Error != nil {
			if copy.transfer.Info.Error.Code == syscall.ECANCELED {
				copy.transfer.State = tasks.TransferCanceled
			} else {
				copy.transfer.State = tasks.TransferFailed
			}
			if copy.transfer.Info.Error.Recoverable {
				log.Errorf("Recoverable error: [%d] %s",
					copy.transfer.Info.Error.Code, copy.transfer.Info.Error.Description)
			} else {
				log.Errorf("Non recoverable error: [%d] %s",
					copy.transfer.Info.Error.Code, copy.transfer.Info.Error.Description)
			}
			copy.failures++
		} else {
			copy.transfer.State = tasks.TransferFinished
			log.Info("Transfer finished successfully")
		}

		if copy.batch.Type == tasks.BatchMultihop || copy.batch.Type == tasks.BatchMultisource {
			break
		}
	}

	copy.setStateForRemaining()
	copy.reportBatchEnd()
}

// Triggers a graceful cancellation.
func (copy *urlCopy) Cancel() {
	copy.context.Cancel()
	copy.canceled = true
}

// Ungracefully terminates the transfers. It doesn't even bother sending a Cancel, since
// the underlying gfal2 handler may be in an inconsistent state and the reason for the Panic.
// It tries its best to send a termination message for all non-executed transfers.
func (copy *urlCopy) Panic(format string, args ...interface{}) {
	// Common error for all
	message := fmt.Sprintf(format, args...)
	error := &tasks.TransferError{
		Scope:       tasks.ScopeAgent,
		Code:        syscall.EINTR,
		Description: message,
		Recoverable: false,
	}
	// Not run yet
	for transfer := copy.next(); transfer != nil; transfer = copy.next() {
		transfer.State = tasks.TransferFailed
		transfer.Info.Error = error
	}
	// The one running
	if copy.transfer != nil {
		copy.transfer.State = tasks.TransferFailed
		copy.transfer.Info.Error = error
	}
	copy.reportBatchEnd()
}
