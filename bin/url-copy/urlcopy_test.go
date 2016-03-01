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
	"github.com/Sirupsen/logrus"
	"gitlab.cern.ch/flutter/fts/messages"
	"gitlab.cern.ch/flutter/fts/types"
	"gitlab.cern.ch/flutter/fts/types/surl"
	"gitlab.cern.ch/flutter/go-dirq"
	"io/ioutil"
	"log"
	"os"
	"path"
	"syscall"
	"testing"
	"time"
)

func init() {
	*logToStderr = true
	*dirqBasePath = "/tmp/url_copy_tests/lib"
	*logBaseDir = "/tmp/url_copy_tests/log"
	logrus.SetLevel(logrus.FatalLevel)

	os.RemoveAll(*dirqBasePath)
	os.RemoveAll(*logBaseDir)
}

func surlHelper(raw string) (parsed surl.SURL) {
	if surl, err := surl.Parse(raw); err != nil {
		log.Panic(err)
	} else {
		parsed = *surl
	}
	return parsed
}

// Serialize serializes the task into a temporary file, so it can be passed to the
// copy function.
func Serialize(t *testing.T, task *types.Batch) (file string) {
	bytes, err := json.Marshal(task)
	if err != nil {
		t.Error(err)
	}

	f, err := ioutil.TempFile(os.TempDir(), "url_copy")
	if err != nil {
		t.Error(err)
	}

	f.Write(bytes)
	f.Close()
	return f.Name()
}

// ConsumeStartMessages reads the start messages generated by url_copy
func ConsumeStartMessages(t *testing.T) []*messages.TransferStart {
	startList := make([]*messages.TransferStart, 0, 10)

	startDirq, err := dirq.New(path.Join(*dirqBasePath, "start"))
	if err != nil {
		t.Error(err)
	}

	var start *messages.TransferStart
	for msg := range startDirq.Consume() {
		if msg.Error == nil {
			start = new(messages.TransferStart)
			err = json.Unmarshal(msg.Message, start)
			if err != nil {
				t.Error(err)
			} else {
				startList = append(startList, start)
			}
		} else {
			t.Error(msg.Error)
		}
	}

	return startList
}

// ConsumeEndMessages reads the end messages generated by url_copy
func ConsumeEndMessages(t *testing.T) []*messages.TransferEnd {
	endList := make([]*messages.TransferEnd, 0, 10)

	endDirq, err := dirq.New(path.Join(*dirqBasePath, "end"))
	if err != nil {
		t.Error(err)
	}

	var end *messages.TransferEnd
	for msg := range endDirq.Consume() {
		if msg.Error == nil {
			end = new(messages.TransferEnd)
			err = json.Unmarshal(msg.Message, end)
			if err != nil {
				t.Error(err)
			} else {
				endList = append(endList, end)
			}
		} else {
			t.Error(msg.Error)
		}
	}

	return endList
}

// Test a simple transfer, with two files, default timeout (which is long enough)
// Once the transfer is finished, there must be one start message, and one end message.
func TestSimpleTransfer(t *testing.T) {
	transfer := &types.Transfer{
		JobId:       "d025fb26-2279-11e6-a607-02163e006dd0",
		TransferId:  "e0ccca86-2279-11e6-9c7b-02163e006dd0",
		Source:      surlHelper("mock://host/path?size=10"),
		Destination: surlHelper("mock://host/path?size_post=10&time=1"),
	}

	task := &types.Batch{
		Type:      types.BatchBulk,
		Transfers: []*types.Transfer{transfer},
	}

	path := Serialize(t, task)
	copy := NewUrlCopy(path)
	copy.Run()

	start := ConsumeStartMessages(t)
	end := ConsumeEndMessages(t)

	if len(start) != 1 {
		t.Errorf("Expecting 1 start message, got %d", len(start))
		return
	}
	if len(end) != 1 {
		t.Errorf("Expecting 1 end message, got %d", len(end))
		return
	}

	if start[0].JobId != transfer.JobId || start[0].TransferId != transfer.TransferId {
		t.Errorf("Received transfer on start message does not match submitted")
		t.Log(start[0])
		t.Log(transfer)
	}
	if end[0].JobId != transfer.JobId || end[0].TransferId != transfer.TransferId {
		t.Errorf("Received transfer on end message does not match submitted")
		t.Log(end[0])
		t.Log(transfer)
	}
	if end[0].Error != nil {
		t.Errorf("Expecting success, got %s", end[0].Error.Description)
	}
	if end[0].Stats.FileSize != 10 {
		t.Errorf("Expecting filesize to be reported 10, got %d", end[0].Stats.FileSize)
	}
}

// Test the Panic method without running. There must be one end message after the fact.
func TestPanic(t *testing.T) {
	transfer := &types.Transfer{
		JobId:       "d025fb26-2279-11e6-a607-02163e006dd0",
		TransferId:  "e0ccca86-2279-11e6-9c7b-02163e006dd0",
		Source:      surlHelper("mock://host/path?size=10"),
		Destination: surlHelper("mock://host/path?size_post=10&time=1"),
	}

	task := &types.Batch{
		Type:      types.BatchBulk,
		Transfers: []*types.Transfer{transfer},
	}

	path := Serialize(t, task)
	copy := NewUrlCopy(path)
	copy.Panic("TEST PANIC MESSAGE")

	end := ConsumeEndMessages(t)

	if len(end) != 1 {
		t.Errorf("Expecting 1 end message, got %d", len(end))
		return
	}

	if end[0].JobId != transfer.JobId || end[0].TransferId != transfer.TransferId {
		t.Errorf("Received transfer on end message does not match submitted")
		t.Log(end[0])
		t.Log(transfer)
	}

	if end[0].Error == nil {
		t.Errorf("Expecting an error, got none")
	} else if end[0].Error.Code != syscall.EINTR {
		t.Errorf("Expecting EINTR, got %d", end[0].Error.Code)
	} else if end[0].Error.Description != "TEST PANIC MESSAGE" {
		t.Errorf("Unexpected error message, got %s", end[0].Error.Description)
	}
}

// Test cancelling a file transfer in the middle of it.
// There must be one start and one end messages. The end message must be set properly.
func TestCancel(t *testing.T) {
	transfer := &types.Transfer{
		JobId:       "d025fb26-2279-11e6-a607-02163e006dd0",
		TransferId:  "e0ccca86-2279-11e6-9c7b-02163e006dd0",
		Source:      surlHelper("mock://host/path?size=10"),
		Destination: surlHelper("mock://host/path?size_post=10&time=5"),
	}

	task := &types.Batch{
		Type:      types.BatchBulk,
		Transfers: []*types.Transfer{transfer},
	}

	path := Serialize(t, task)
	copy := NewUrlCopy(path)
	go func() {
		time.Sleep(1 * time.Second)
		copy.Cancel()
	}()
	copy.Run()

	start := ConsumeStartMessages(t)
	end := ConsumeEndMessages(t)

	if len(start) != 1 {
		t.Errorf("Expecting 1 start message, got %d", len(start))
		return
	}
	if len(end) != 1 {
		t.Errorf("Expecting 1 end message, got %d", len(end))
		return
	}

	if start[0].JobId != transfer.JobId || start[0].TransferId != transfer.TransferId {
		t.Errorf("Received transfer on start message does not match submitted")
		t.Log(start[0])
		t.Log(transfer)
	}
	if end[0].JobId != transfer.JobId || end[0].TransferId != transfer.TransferId {
		t.Errorf("Received transfer on end message does not match submitted")
		t.Log(end[0])
		t.Log(transfer)
	}
	if end[0].Error == nil {
		t.Errorf("Expecting an error, got success")
		return
	} else if end[0].Error.Code != syscall.ECANCELED {
		t.Errorf("Expecting ECANCELED, got %d", end[0].Error.Code)
	}
}

// Test the timeout function. Set a shorter timeout than the time it will take to transfer.
// The transfer must finish with a timeout end message.
func TestTimeout(t *testing.T) {
	timeout := 1
	transfer := &types.Transfer{
		JobId:       "d025fb26-2279-11e6-a607-02163e006dd0",
		TransferId:  "e0ccca86-2279-11e6-9c7b-02163e006dd0",
		Source:      surlHelper("mock://host/path?size=10"),
		Destination: surlHelper("mock://host/path?size_post=10&time=10"),
		Parameters: types.TransferParameters{
			Timeout: timeout,
		},
	}

	task := &types.Batch{
		Type:      types.BatchBulk,
		Transfers: []*types.Transfer{transfer},
	}

	path := Serialize(t, task)
	copy := NewUrlCopy(path)
	copy.Run()

	start := ConsumeStartMessages(t)
	end := ConsumeEndMessages(t)

	if len(start) != 1 {
		t.Errorf("Expecting 1 start message, got %d", len(start))
		return
	}
	if len(end) != 1 {
		t.Errorf("Expecting 1 end message, got %d", len(end))
		return
	}

	if start[0].JobId != transfer.JobId || start[0].TransferId != transfer.TransferId {
		t.Errorf("Received transfer on start message does not match submitted")
		t.Log(start[0])
		t.Log(transfer)
	}
	if end[0].JobId != transfer.JobId || end[0].TransferId != transfer.TransferId {
		t.Errorf("Received transfer on end message does not match submitted")
		t.Log(end[0])
		t.Log(transfer)
	}
	if end[0].Error == nil {
		t.Errorf("Expecting an error, got success")
	} else if end[0].Error.Code != syscall.ETIMEDOUT {
		t.Errorf("Expecting ETIMEDOUT, got %d", end[0].Error.Code)
	}
}

// Test a simple bulk job: two transfers in one url_copy process. There must be two
// start and two end messages, both succesful.
func TestMultipleSimple(t *testing.T) {
	transfer1 := &types.Transfer{
		JobId:       "d025fb26-2279-11e6-a607-02163e006dd0",
		TransferId:  "e0ccca86-2279-11e6-9c7b-02163e006dd0",
		Source:      surlHelper("mock://host/path?size=10"),
		Destination: surlHelper("mock://host/path?size_post=10&time=1"),
	}
	transfer2 := &types.Transfer{
		JobId:       "d025fb26-2279-11e6-a607-02163e006dd0",
		TransferId:  "1e97ce14-227b-11e6-81e2-02163e006dd0",
		Source:      surlHelper("mock://host/path?size=42"),
		Destination: surlHelper("mock://host/path?size_post=42&time=1"),
	}

	task := &types.Batch{
		Type:      types.BatchBulk,
		Transfers: []*types.Transfer{transfer1, transfer2},
	}

	path := Serialize(t, task)
	copy := NewUrlCopy(path)
	copy.Run()

	start := ConsumeStartMessages(t)
	end := ConsumeEndMessages(t)

	if len(start) != 2 {
		t.Errorf("Expecting 2 start message, got %d", len(start))
		return
	}
	if len(end) != 2 {
		t.Errorf("Expecting 2 end message, got %d", len(end))
		return
	}

	if end[0].Error != nil {
		t.Errorf("First transfer expected success, got %s", end[0].Error.Description)
		return
	}
	if end[1].Error != nil {
		t.Errorf("Second transfer expected success, got %s", end[1].Error.Description)
		return
	}

	if end[0].Stats.FileSize != 10 {
		t.Errorf("File size for transfer 1 does not match. Got %d", end[0].Stats.FileSize)
	}
	if end[1].Stats.FileSize != 42 {
		t.Errorf("File size for transfer 2 does not match. Got %d", end[1].Stats.FileSize)
	}
}

// Test a bulk task, and cancel after a while, giving time to the first transfer to finish.
// There must be two start and two end messages. The first one must be successful and the second one
// must be failed with ECANCELED.
func TestMultipleCancel(t *testing.T) {
	transfer1 := &types.Transfer{
		JobId:       "d025fb26-2279-11e6-a607-02163e006dd0",
		TransferId:  "e0ccca86-2279-11e6-9c7b-02163e006dd0",
		Source:      surlHelper("mock://host/path?size=10"),
		Destination: surlHelper("mock://host/path?size_post=10&time=2"),
	}
	transfer2 := &types.Transfer{
		JobId:       "d025fb26-2279-11e6-a607-02163e006dd0",
		TransferId:  "1e97ce14-227b-11e6-81e2-02163e006dd0",
		Source:      surlHelper("mock://host/path?size=42"),
		Destination: surlHelper("mock://host/path?size_post=42&time=10"),
	}

	task := &types.Batch{
		Type:      types.BatchBulk,
		Transfers: []*types.Transfer{transfer1, transfer2},
	}

	path := Serialize(t, task)
	copy := NewUrlCopy(path)
	go func() {
		time.Sleep(4 * time.Second)
		copy.Cancel()
	}()
	copy.Run()

	start := ConsumeStartMessages(t)
	end := ConsumeEndMessages(t)

	if len(start) != 2 {
		t.Errorf("Expecting 2 start message, got %d", len(start))
		return
	}
	if len(end) != 2 {
		t.Errorf("Expecting 2 end message, got %d", len(end))
		return
	}

	if end[0].Error != nil {
		t.Errorf("First transfer expecting success, got %s", end[0].Error.Description)
	}
	if end[1].Error == nil {
		t.Errorf("Second transfer expecting failure, got success")
	} else if end[1].Error.Code != syscall.ECANCELED {
		t.Errorf("Second transfer expecting ECANCELED, got %d", end[1].Error.Code)
	}
}

// Similar to the cancel test, but this time trigger a Panic, which normally would be
// called by the signal handler. The process normally would terminate immediately, but an
// end message must have been generated.
func TestMultiplePanic(t *testing.T) {
	transfer1 := &types.Transfer{
		JobId:       "d025fb26-2279-11e6-a607-02163e006dd0",
		TransferId:  "e0ccca86-2279-11e6-9c7b-02163e006dd0",
		Source:      surlHelper("mock://host/path?size=10"),
		Destination: surlHelper("mock://host/path?size_post=10&time=2"),
	}
	transfer2 := &types.Transfer{
		JobId:       "d025fb26-2279-11e6-a607-02163e006dd0",
		TransferId:  "1e97ce14-227b-11e6-81e2-02163e006dd0",
		Source:      surlHelper("mock://host/path?size=42"),
		Destination: surlHelper("mock://host/path?size_post=42&time=5"),
	}

	task := &types.Batch{
		Type:      types.BatchBulk,
		Transfers: []*types.Transfer{transfer1, transfer2},
	}

	path := Serialize(t, task)
	copy := NewUrlCopy(path)
	go func() {
		time.Sleep(3 * time.Second)
		copy.Panic("Signal 386")
	}()
	copy.Run()

	start := ConsumeStartMessages(t)
	end := ConsumeEndMessages(t)

	if len(start) != 2 {
		t.Errorf("Expecting 2 start message, got %d", len(start))
		return
	}
	if len(end) != 2 {
		t.Errorf("Expecting 2 end message, got %d", len(end))
		return
	}

	if end[0].Error != nil {
		t.Errorf("First transfer expecting success, got %s", end[0].Error.Description)
	}
	if end[1].Error == nil {
		t.Errorf("Second transfer expecting failure, got success")
	} else if end[1].Error.Code != syscall.EINTR {
		t.Errorf("Second transfer expecting EINTR, got %d", end[1].Error.Code)
	}
}

// Test a multihop transfer. Since the first hop fails, the second must not run, even though it would
// success.
func TestMultiHop(t *testing.T) {
	transfer1 := &types.Transfer{
		JobId:       "d025fb26-2279-11e6-a607-02163e006dd0",
		TransferId:  "e0ccca86-2279-11e6-9c7b-02163e006dd0",
		Source:      surlHelper("mock://host/path?size=10&errno=2"),
		Destination: surlHelper("mock://host/path?size_post=10&time=2"),
	}
	transfer2 := &types.Transfer{
		JobId:       "d025fb26-2279-11e6-a607-02163e006dd0",
		TransferId:  "1e97ce14-227b-11e6-81e2-02163e006dd0",
		Source:      surlHelper("mock://host/path?size=42"),
		Destination: surlHelper("mock://host/path?size_post=42&time=2"),
	}

	task := &types.Batch{
		Type:      types.BatchMultihop,
		Transfers: []*types.Transfer{transfer1, transfer2},
	}

	path := Serialize(t, task)
	copy := NewUrlCopy(path)
	copy.Run()

	start := ConsumeStartMessages(t)
	end := ConsumeEndMessages(t)

	if len(start) != 1 {
		t.Errorf("Expecting 1 start message, got %d", len(start))
		for _, s := range start {
			t.Log(s)
		}
		return
	}
	if len(end) != 2 {
		t.Errorf("Expecting 2 end message, got %d", len(end))
		return
	}

	if end[0].Error == nil {
		t.Errorf("First transfer expecting failure, got success")
	} else if end[0].Error.Code != syscall.ENOENT {
		t.Errorf("First transfer expecting ENOENT, got %d instead", end[0].Error.Code)
	}
	if end[1].Error == nil {
		t.Errorf("Second transfer expecting failure, got success")
	} else if end[1].Error.Code != syscall.ECANCELED {
		t.Errorf("Second transfer expecting ECANCELED, got %d instead", end[0].Error.Code)
	}
}
