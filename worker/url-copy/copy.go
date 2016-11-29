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
	"gitlab.cern.ch/flutter/fts/messages"
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
	batch         messages.Batch
	transferIndex int
	terminalSent  bool

	// Transfer being run
	transfer *messages.Transfer
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
		copy.batch.Transfers[index].Info = &messages.TransferInfo{}
	}

	copy.context, err = gfal2.NewContext()
	if err != nil {
		copy.Panic("Could not instantiate the gfal2 context: %s", err.Error())
	}
	return &copy
}

// next returns the next transfer in the list, nil on end
func (copy *urlCopy) next() *messages.Transfer {
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
func (copy *urlCopy) setupGfal2ForTransfer(transfer *messages.Transfer) {
	copy.context.SetOptBoolean("GRIDFTP PLUGIN", "SESSION_REUSE", true)
	copy.context.SetUserAgent("url_copy", version.Version)
	if *x509proxy != "" {
		copy.context.SetOptString("X509", "CERT", *x509proxy)
		copy.context.SetOptString("X509", "KEY", *x509proxy)
	} else {
		copy.context.SetOptString("X509", "CERT", fmt.Sprintf("/tmp/proxy_%s.pem", copy.batch.CredId))
		copy.context.SetOptString("X509", "KEY", fmt.Sprintf("/tmp/proxy_%s.pem", copy.batch.CredId))
	}

	if transfer.Parameters != nil {
		copy.context.SetOptBoolean("GRIDFTP PLUGIN", "ENABLE_UDT", transfer.Parameters.EnableUdt)
		copy.context.SetOptBoolean("GRIDFTP PLUGIN", "IPV6", transfer.Parameters.EnableIpv6)
	}
}

// createCopyHandler creates and prepares a gfal2 copy handler to run the transfer.
func (copy *urlCopy) createCopyHandler(transfer *messages.Transfer) (handler *gfal2.TransferHandler, err gfal2.GError) {
	handler, err = copy.context.NewTransferHandler()
	if err != nil {
		return
	}

	handler.SetCreateParentDir(true)
	if transfer.Parameters != nil {
		handler.SetStrictCopy(transfer.Parameters.OnlyCopy)
		handler.SetOverwrite(transfer.Parameters.Overwrite)
		handler.SetSourceSpacetoken(transfer.Parameters.SourceSpacetoken)
		handler.SetDestinationSpaceToken(transfer.Parameters.DestSpacetoken)

		checksumMode := 0
		switch transfer.Parameters.ChecksumMode {
		case messages.TransferParameters_SOURCE:
			checksumMode = gfal2.ChecksumSource
		case messages.TransferParameters_TARGET:
			checksumMode = gfal2.ChecksumTarget
		case messages.TransferParameters_END2END:
			checksumMode = gfal2.ChecksumBoth
		}
		if checksumMode != 0 {
			parts := strings.SplitN(transfer.Checksum, ":", 2)
			if len(parts) == 2 {
				handler.SetChecksum(checksumMode, parts[0], parts[1])
			} else {
				log.Warning("Invalid checksum passed. Ignoring.")
			}

		}
	}

	handler.AddMonitorCallback(copy)
	return
}

// Called by gfal2 when there are performance markers available.
func (copy *urlCopy) NotifyPerformanceMarker(marker gfal2.Marker) {
	perf := &messages.PerformanceMarker{
		TransferId:  copy.transfer.TransferId,
		Throughput:  float32(marker.AvgThroughput),
		Transferred: marker.BytesTransferred,
		SourceSe:    "TODO",
		DestSe:      "TODO",
	}
	if err := copy.reportPerformance(perf); err != nil {
		log.Error(err)
	}
	copy.transfer.Info.Stats.Transferred = marker.BytesTransferred
	copy.transfer.Info.Stats.Throughput = float32(marker.AvgThroughput)
}

// runTransfer prepares the context and copy handler, and run the transfer. It sends the start message,
// and returns the termination message, but it does not send it.
func (copy *urlCopy) runTransfer(transfer *messages.Transfer) {
	var err error

	transfer.Info = &messages.TransferInfo{
		Error:   nil,
		Message: "Starting transfer",
		Stats: &messages.TransferRunStatistics{
			Intervals: &messages.TransferIntervals{},
		},
		LogFile: "",
	}
	transfer.Info.Stats.Intervals.Total = &messages.Interval{
		Start: messages.Now(),
	}

	logFile, err := generateLogPath(transfer)
	if err != nil {
		copy.Panic("Could not create the log file: %s", err.Error())
	}
	if !*logToStderr {
		redirectLog(logFile)
		transfer.Info.LogFile = logFile
	}

	log.Info("Transfer accepted")
	if err != nil {
		log.Errorf("Failed to send the start message: %s", err.Error())
	}

	copy.setupGfal2ForTransfer(transfer)
	params, gerr := copy.createCopyHandler(transfer)
	if gerr != nil {
		transfer.Info.Error = &messages.TransferError{
			Scope:       messages.TransferError_AGENT,
			Code:        int32(gerr.Code()),
			Description: gerr.Error(),
			Recoverable: false,
		}
		return
	}

	log.Infof("Proxy: %s", copy.context.GetOptString("X509", "CERT"))
	log.Infof("Transfer id: %s", transfer.TransferId)
	log.Infof("Source url: %s", transfer.Source)
	log.Infof("Dest url: %s", transfer.Destination)
	log.Infof("Overwrite enabled: %t", params.GetOverwrite())
	if transfer.Parameters != nil {
		log.Infof("Dest space token: %s", transfer.Parameters.DestSpacetoken)
		log.Infof("Source space token: %s", transfer.Parameters.SourceSpacetoken)
	}
	log.Infof("Checksum: %s", transfer.Checksum)
	log.Infof("User filesize: %d", transfer.Filesize)

	log.Infof("IPv6: %t", copy.context.GetOptBoolean("GRIDFTP PLUGIN", "IPV6"))
	log.Infof("UDT: %t", copy.context.GetOptBoolean("GRIDFTP PLUGIN", "ENABLE_UDT"))
	if copy.context.GetOptBoolean("BDII", "ENABLED") {
		log.Infof("BDII: %s", copy.context.GetOptString("BDII", "LCG_GFAL_INFOSYS"))
	}

	srcStat, gerr := copy.context.Stat(transfer.Source)
	if gerr != nil {
		transfer.Info.Error = &messages.TransferError{
			Scope:       messages.TransferError_SOURCE,
			Code:        int32(gerr.Code()),
			Description: gerr.Error(),
			Recoverable: IsRecoverable(messages.TransferError_SOURCE, gerr.Code()),
		}
		return
	}

	log.Infof("File size: %d", srcStat.Size())

	if transfer.Filesize != 0 && transfer.Filesize != uint64(srcStat.Size()) {
		transfer.Info.Error = &messages.TransferError{
			Scope: messages.TransferError_SOURCE,
			Code:  int32(syscall.EINVAL),
			Description: fmt.Sprintf(
				"Source and user provide file sizes do not match: %d != %d",
				srcStat.Size(), transfer.Filesize,
			),
			Recoverable: false,
		}
		return
	} else {
		transfer.Filesize = uint64(srcStat.Size())
	}

	var timeout time.Duration
	if transfer.Parameters != nil && transfer.Parameters.Timeout != nil && transfer.Parameters.Timeout.Seconds > 0 {
		timeout = time.Duration(transfer.Parameters.Timeout.Seconds)
	} else {
		timeout = AdjustTimeoutBasedOnSize(srcStat.Size()) + (600 * time.Second)
	}
	log.Infof("Timeout: %.0f seconds", timeout.Seconds())

	// Run the copy in a separate goroutine so we can catch timeouts
	done := make(chan bool, 1)
	go func() {
		gerr = params.CopyFile(transfer.Source, transfer.Destination)
		done <- true
	}()

	select {
	case _ = <-done:
	case <-time.After(timeout):
		copy.context.Cancel()
		transfer.Info.Error = &messages.TransferError{
			Scope:       messages.TransferError_TRANSFER,
			Code:        int32(syscall.ETIMEDOUT),
			Description: "Transfer timed out",
			Recoverable: true,
		}
		return
	}

	transfer.Info.Stats.Intervals.Total.End = messages.Now()

	if gerr != nil {
		transfer.Info.Error = &messages.TransferError{
			Scope:       messages.TransferError_TRANSFER,
			Code:        int32(gerr.Code()),
			Description: gerr.Error(),
			Recoverable: IsRecoverable(messages.TransferError_TRANSFER, gerr.Code()),
		}
	} else {
		// Adjust size and throughput if the protocol didn't give us perf.markers
		if transfer.Info.Stats.Transferred == 0 {
			transfer.Info.Stats.Transferred = uint64(srcStat.Size())
		}
		if transfer.Info.Stats.Throughput == 0 {
			transfer.Info.Stats.Throughput = float32(
				float64(srcStat.Size()) / (transfer.Info.Stats.Intervals.Total.Elapsed().Seconds()),
			)
		}
	}
	return
}

// sendTerminalForRemaining send a terminal message for any transfer than hasn't run yet.
// This could be due to external cancellation, or multihop failures.
func (copy *urlCopy) setStateForRemaining() {
	// TODO
	var remainingInfo *messages.TransferInfo
	var remainingState messages.Transfer_State

	remainingState = messages.Transfer_FAILED
	remainingInfo = &messages.TransferInfo{
		Error: &messages.TransferError{
			Scope:       messages.TransferError_AGENT,
			Code:        int32(syscall.ECANCELED),
			Description: "Transfer canceled because a previous hop failed",
			Recoverable: false,
		},
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
			if copy.transfer.Info.Error.Code == int32(syscall.ECANCELED) {
				copy.transfer.State = messages.Transfer_CANCELED
			} else {
				copy.transfer.State = messages.Transfer_FAILED
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
			copy.transfer.State = messages.Transfer_FINISHED
			log.Info("Transfer finished successfully")
		}

		// TODO: Multihop, multisources
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
	error := &messages.TransferError{
		Scope:       messages.TransferError_AGENT,
		Code:        int32(syscall.EINTR),
		Description: message,
		Recoverable: false,
	}
	// Not run yet
	for transfer := copy.next(); transfer != nil; transfer = copy.next() {
		transfer.State = messages.Transfer_FAILED
		transfer.Info.Error = error
	}
	// The one running
	if copy.transfer != nil {
		copy.transfer.State = messages.Transfer_FAILED
		copy.transfer.Info.Error = error
	}
	copy.reportBatchEnd()
}
