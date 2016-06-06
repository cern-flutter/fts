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
	"gitlab.cern.ch/flutter/fts/types/interval"
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

type UrlCopy struct {
	context        *gfal2.Context
	canceled       bool
	multihopFailed bool

	mutex       sync.Mutex
	transferSet tasks.Batch

	// Transfer being run
	transfer *tasks.Transfer
}

// NewUrlCopy creates a new instance of UrlCopy, initialized using the task
// serialized in the file pointed by taskfile.
func NewUrlCopy(taskfile string) *UrlCopy {
	var err error
	copy := UrlCopy{}

	raw, err := ioutil.ReadFile(taskfile)
	if err != nil {
		log.Panicf("Could not open the task file: %s", err.Error())
	}
	err = json.Unmarshal(raw, &copy.transferSet)
	if err != nil {
		log.Panicf("Failed to parse the task file: %s", err.Error())
	}

	if !*keepTaskFile {
		os.Remove(taskfile)
	}

	copy.context, err = gfal2.NewContext()
	if err != nil {
		copy.Panic("Could not instantiate the gfal2 context: %s", err.Error())
	}
	return &copy
}

// getFront returns the first transfer pending on the queue.
// It does *not* remove it.
func (copy *UrlCopy) getFront() *tasks.Transfer {
	copy.mutex.Lock()
	defer copy.mutex.Unlock()
	if len(copy.transferSet.Transfers) == 0 {
		return nil
	}
	return copy.transferSet.Transfers[0]
}

// popFront removes the first transfer from the queue.
// It returns true if it was removed, false if the queue was already empty.
func (copy *UrlCopy) popFront() bool {
	copy.mutex.Lock()
	defer copy.mutex.Unlock()
	if len(copy.transferSet.Transfers) > 0 {
		copy.transferSet.Transfers = copy.transferSet.Transfers[1:]
		return true
	}
	return false
}

// setupGfal2ForTransfer prepares the gfal2 context for this particular transfer.
func (copy *UrlCopy) setupGfal2ForTransfer(transfer *tasks.Transfer) {
	copy.context.SetOptBoolean("GRIDFTP PLUGIN", "SESSION_REUSE", true)
	copy.context.SetOptBoolean("GRIDFTP PLUGIN", "ENABLE_UDT", transfer.Parameters.EnableUdt)
	copy.context.SetOptBoolean("GRIDFTP PLUGIN", "IPV6", transfer.Parameters.EnableIpv6)
	copy.context.SetUserAgent("url_copy", version.Version)
	copy.context.SetOptString("X509", "CERT", fmt.Sprintf("/tmp/proxy_%s.pem", copy.transferSet.DelegationID))
	copy.context.SetOptString("X509", "KEY", fmt.Sprintf("/tmp/proxy_%s.pem", copy.transferSet.DelegationID))

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
func (copy *UrlCopy) createCopyHandler(transfer *tasks.Transfer) (handler *gfal2.TransferHandler, err gfal2.GError) {
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
func (copy *UrlCopy) NotifyPerformanceMarker(marker gfal2.Marker) {
	perf := &perf.Marker{
		JobID:              copy.transfer.JobID,
		TransferID:         copy.transfer.TransferID,
		SourceStorage:      copy.transfer.Source.GetStorageName(),
		DestinationStorage: copy.transfer.Destination.GetStorageName(),
		Throughput:         marker.AvgThroughput,
		TransferredBytes:   marker.BytesTransferred,
	}
	if err := ReportPerformance(perf); err != nil {
		log.Error(err)
	}
}

// runTransfer prepares the context and copy handler, and run the transfer. It sends the start message,
// and returns the termination message, but it does not send it.
func (copy *UrlCopy) runTransfer(transfer *tasks.Transfer) {
	var err error

	transfer.Status = &tasks.TransferStatus{
		State:   tasks.Active,
		Error:   nil,
		Message: "Starting transfer",
		Stats:   nil,
		LogFile: nil,
	}

	logFile, err := generateLogPath(transfer)
	if err != nil {
		copy.Panic("Could not create the log file: %s", err.Error())
	}
	if !*logToStderr {
		redirectLog(logFile)
		transfer.Status.LogFile = &logFile
	}

	log.Info("Transfer accepted")
	err = ReportStart(transfer)
	if err != nil {
		log.Errorf("Failed to send the start message: %s", err.Error())
	}

	transfer.Status.Stats = &tasks.TransferRunStatistics{
		Intervals: tasks.TransferIntervals{
			Total: interval.Interval{
				Start: time.Now(),
			},
		},
	}

	copy.setupGfal2ForTransfer(transfer)
	params, gerr := copy.createCopyHandler(transfer)
	if gerr != nil {
		transfer.Status.Error = &tasks.TransferError{
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

	log.Infof("Multihop: %t", copy.transferSet.Type == tasks.BatchMultihop)
	log.Infof("IPv6: %t", copy.context.GetOptBoolean("GRIDFTP PLUGIN", "IPV6"))
	log.Infof("UDT: %t", copy.context.GetOptBoolean("GRIDFTP PLUGIN", "ENABLE_UDT"))
	if copy.context.GetOptBoolean("BDII", "ENABLED") {
		log.Infof("BDII: %s", copy.context.GetOptString("BDII", "LCG_GFAL_INFOSYS"))
	}

	srcStat, gerr := copy.context.Stat(transfer.Source.String())
	if gerr != nil {
		transfer.Status.Error = &tasks.TransferError{
			Scope:       tasks.ScopeSource,
			Code:        gerr.Code(),
			Description: gerr.Error(),
			Recoverable: IsRecoverable(tasks.ScopeSource, gerr.Code()),
		}
		return
	}

	log.Infof("File size: %d", srcStat.Size())

	if transfer.Filesize != nil && *transfer.Filesize != 0 && *transfer.Filesize != srcStat.Size() {
		transfer.Status.Error = &tasks.TransferError{
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
		transfer.Status.Error = &tasks.TransferError{
			Scope:       tasks.ScopeTransfer,
			Code:        syscall.ETIMEDOUT,
			Description: "Transfer timed out",
			Recoverable: true,
		}
		return
	}

	transfer.Status.Stats.Intervals.Total.End = time.Now()

	if gerr != nil {
		transfer.Status.Error = &tasks.TransferError{
			Scope:       tasks.ScopeTransfer,
			Code:        gerr.Code(),
			Description: gerr.Error(),
			Recoverable: IsRecoverable(tasks.ScopeTransfer, gerr.Code()),
		}
		return
	}
	return
}

// sendTerminalForRemaining send a terminal message for any transfer than hasn't run yet.
// This could be due to external cancellation, or multihop failures.
func (copy *UrlCopy) sendTerminalForRemaining() {
	copy.mutex.Lock()
	defer copy.mutex.Unlock()

	var description string
	if copy.multihopFailed {
		description = "Transfer canceled because a previous hop failed"
	} else {
		description = "Transfer canceled"
	}

	for _, transfer := range copy.transferSet.Transfers {
		transfer.Status = &tasks.TransferStatus{
			Error: &tasks.TransferError{
				Scope:       tasks.ScopeTransfer,
				Code:        syscall.ECANCELED,
				Description: description,
				Recoverable: false,
			},
			Stats:   nil,
			LogFile: nil,
		}
		err := ReportTerminal(transfer)
		if err != nil {
			log.Errorf("Failed to send the terminal message: %s", err.Error())
		}
	}
}

// Run runs the whole process.
func (copy *UrlCopy) Run() {
	for !copy.canceled && !copy.multihopFailed && len(copy.transferSet.Transfers) > 0 {
		copy.transfer = copy.getFront()
		copy.runTransfer(copy.transfer)

		if copy.transfer.Status.Error != nil {
			copy.transfer.Status.State = tasks.Failed
			if copy.transfer.Status.Error.Recoverable {
				log.Errorf("Recoverable error: [%d] %s",
					copy.transfer.Status.Error.Code, copy.transfer.Status.Error.Description)
			} else {
				log.Errorf("Non recoverable error: [%d] %s",
					copy.transfer.Status.Error.Code, copy.transfer.Status.Error.Description)
			}

			if copy.transferSet.Type == tasks.BatchMultihop {
				copy.multihopFailed = true
			}
		} else {
			copy.transfer.Status.State = tasks.Finished
			log.Infof("Transfer finished successfully")
		}

		if copy.popFront() {
			err := ReportTerminal(copy.transfer)
			if err != nil {
				log.Errorf("Failed to send the terminal message: %s", err.Error())
			}
		}
	}

	copy.sendTerminalForRemaining()
}

// Triggers a graceful cancellation.
func (copy *UrlCopy) Cancel() {
	copy.context.Cancel()
	copy.canceled = true
}

// Ungracefully terminates the transfers. It doesn't even bother sending a Cancel, since
// the underlying gfal2 handler may be in an inconsistent state and the reason for the Panic.
// It tries its best to send a termination message for all non-executed transfers.
func (copy *UrlCopy) Panic(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)

	copy.mutex.Lock()
	defer copy.mutex.Unlock()
	for _, transfer := range copy.transferSet.Transfers {
		transfer.Status = &tasks.TransferStatus{
			Error: &tasks.TransferError{
				Scope:       tasks.ScopeAgent,
				Code:        syscall.EINTR,
				Description: message,
				Recoverable: false,
			},
			Stats:   nil,
			LogFile: nil,
		}
		err := ReportTerminal(transfer)
		if err != nil {
			log.Errorf("Failed to send the terminal message inside Panic: %s", err.Error())
		}
	}
	copy.transferSet.Transfers = make([]*tasks.Transfer, 0)
}
