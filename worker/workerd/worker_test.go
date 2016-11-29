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
	"github.com/satori/go.uuid"
	"gitlab.cern.ch/flutter/fts/messages"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

var localDbTestPath = "/tmp/worker-test.db"

func TestKillProc(t *testing.T) {
	supervisor, err := NewSupervisor(localDbTestPath)
	if err != nil {
		t.Fatal(err)
	}
	defer supervisor.Close()

	cmd := exec.Command("sleep", "100s")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	supervisor.RegisterProcess(&messages.Batch{}, cmd.Process.Pid)
	time.Sleep(500 * time.Millisecond)

	supervisor.Kill(cmd.Process.Pid)

	// killProc should have consumed the wait from the child
	if err := cmd.Wait(); err == nil {
		t.Fatal("Expecting an error")
	} else if err.(*os.SyscallError).Err.(syscall.Errno) != syscall.ECHILD {
		t.Fatal("Expecting ECHILD, got ", err)
	}
}

func TestSigKill(t *testing.T) {
	supervisor, err := NewSupervisor(localDbTestPath)
	if err != nil {
		t.Fatal(err)
	}
	defer supervisor.Close()

	cmd := exec.Command("bash", "-c", "_term() { echo trap; sleep 100; }; trap _term SIGTERM; echo XXXX; sleep 100")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	supervisor.RegisterProcess(&messages.Batch{}, cmd.Process.Pid)
	time.Sleep(500 * time.Millisecond)

	supervisor.Kill(cmd.Process.Pid)

	// It is acceptable to either get an error (gone), or the final state
	var ws syscall.WaitStatus
	if err := cmd.Wait(); err == nil {
		ws = cmd.ProcessState.Sys().(syscall.WaitStatus)
	} else if syscallErr, ok := err.(*os.SyscallError); ok {
		if syscallErr.Err.(syscall.Errno) != syscall.ECHILD {
			t.Fatal("Expecting ECHILD")
		}
		return
	} else {
		ws = err.(*exec.ExitError).Sys().(syscall.WaitStatus)
	}

	if !ws.Signaled() {
		t.Fatal("Expecting the process to be signaled")
	}
	if ws.Signal() != syscall.SIGKILL {
		t.Fatal("Expecting the signal to be SIGKILL")
	}
}

func TestStoreBatch(t *testing.T) {
	os.RemoveAll(localDbTestPath)
	supervisor, err := NewSupervisor(localDbTestPath)
	if err != nil {
		t.Fatal(err)
	}
	defer supervisor.Close()

	batch1 := &messages.Batch{
		Submitted: messages.Now(),
		State:     messages.Batch_READY,
		Transfers: []*messages.Transfer{{TransferId: uuid.NewV4().String()}},
	}
	pid1 := 64
	batch2 := &messages.Batch{
		Submitted: messages.Now(),
		State:     messages.Batch_READY,
		Transfers: []*messages.Transfer{{TransferId: uuid.NewV4().String()}},
	}
	pid2 := 896

	if err := supervisor.storeProcess(batch1, pid1); err != nil {
		t.Fatal(err)
	}
	if err := supervisor.storeProcess(batch2, pid2); err != nil {
		t.Fatal(err)
	}

	pids := supervisor.GetPidsForKillTask(&messages.Kill{TransferId: batch1.Transfers[0].TransferId})
	if pids[0] != pid1 {
		t.Fatal("Expecting ", pid1, " got ", pids[0])
	}

	if err := supervisor.delete(pid1); err != nil {
		t.Fatal(err)
	}
	pids = supervisor.GetPidsForKillTask(&messages.Kill{TransferId: batch1.Transfers[0].TransferId})
	if len(pids) != 0 {
		t.Fatal("Batch should have been removed")
	}
}
