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
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/syndtr/goleveldb/leveldb"
	"gitlab.cern.ch/flutter/fts/messages"
	"golang.org/x/sys/unix"
	"os"
	"strconv"
	"syscall"
	"time"
)

type (
	procGone struct {
		pid    int
		status syscall.WaitStatus
		errno  syscall.Errno
		error  error
	}

	// Supervisor watches url copy processes
	Supervisor struct {
		Timeout time.Duration
		db      *leveldb.DB
		gone    chan procGone
	}
)

// NewSupervisor opens the local db
func NewSupervisor(path string) (*Supervisor, error) {
	leveldb, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	superv := &Supervisor{
		db:      leveldb,
		Timeout: time.Second * 5,
		gone:    make(chan procGone, 10),
	}
	return superv, superv.recover()
}

// Close closes the local DB
func (superv *Supervisor) Close() {
	superv.db.Close()
}

// recover reads the DB to spawn a watcher per stored process
func (superv *Supervisor) recover() error {
	iter := superv.db.NewIterator(nil, nil)
	for iter.Next() {
		if pid, err := strconv.Atoi(string(iter.Key())); err != nil {
			log.Warn("Failed to recover an entry from the pid database")
		} else {
			go superv.watch(pid)
		}
	}
	return nil
}

// reaper consumes gone messages, and remove from the db
func (superv *Supervisor) Run() {
	log.Info("Supervisor started")
	for gone := range superv.gone {
		if gone.error == nil {
			log.Info("Process ", gone.pid, " finished with ", gone.status)
		} else {
			log.Warn("Process ", gone.pid, " gone, got error ", gone.error)
		}
		if err := superv.delete(gone.pid); err != nil {
			log.WithError(err).Error("Failed to delete pid from the database")
		}
	}
	log.Info("Supervisor finished")
}

// watch waits for the process and generates an event when done
func (superv *Supervisor) watch(pid int) {
	var status syscall.WaitStatus
	var usage syscall.Rusage
	log.Info("Watching ", pid)

	for {
		_, err := syscall.Wait4(pid, &status, 0, &usage)
		if err == nil {
			// Normal exit
			superv.gone <- procGone{
				pid:    pid,
				status: status,
			}
		} else if syscallErr, ok := err.(*os.SyscallError); ok {
			errno := syscallErr.Err.(syscall.Errno)
			if errno == syscall.EINTR {
				continue
			}
			superv.gone <- procGone{
				pid:   pid,
				errno: errno,
				error: syscallErr,
			}
		} else {
			superv.gone <- procGone{
				pid:   pid,
				error: err,
			}
		}
		log.Info("Watcher for ", pid, " done")
		return
	}
}

func (superv *Supervisor) storeProcess(batch *messages.Batch, pid int) error {
	data, err := proto.Marshal(batch)
	if err != nil {
		return err
	}
	pidStr := fmt.Sprint(pid)
	log.Debug("Storing batch with pid ", pid)
	return superv.db.Put([]byte(pidStr), data, nil)
}

// RegisterProcess stores a batch together with its pid on the local db
func (superv *Supervisor) RegisterProcess(batch *messages.Batch, pid int) error {
	go superv.watch(pid)
	return superv.storeProcess(batch, pid)
}

// delete deletes the pid form the internal db
func (superv *Supervisor) delete(pid int) error {
	log.Debug("Delete ", pid)
	pidStr := fmt.Sprint(pid)
	return superv.db.Delete([]byte(pidStr), nil)
}

// GetPidsForKillTask returns the PIDs associated with the batch pointed by the kill task
func (superv *Supervisor) GetPidsForKillTask(kill *messages.Kill) []int {
	pids := make([]int, 0, 1)
	iter := superv.db.NewIterator(nil, nil)
	for iter.Next() {
		var err error
		var batch messages.Batch
		var pid int

		if err = proto.Unmarshal(iter.Value(), &batch); err != nil {
			log.WithError(err).Error("Failed to parse entry in the local db")
			continue
		}
		if pid, err = strconv.Atoi(string(iter.Key())); err != nil {
			log.WithError(err).Error("Failed to parse entry pid in the local db")
		}

		found := false
		for _, transfer := range batch.Transfers {
			found = (kill.TransferId != "" && kill.TransferId == transfer.TransferId)
			if found {
				log.Info("Found kill target ", transfer.TransferId)
				found = true
				break
			}
		}
		if found {
			pids = append(pids, pid)
		}
	}
	return pids
}

// Kill sends first a SIGTERM and then a SIGKILL
func (superv *Supervisor) Kill(pid int) {
	log.Info("Sending SIGTERM to ", pid)
	syscall.Kill(pid, unix.SIGTERM)

	done := make(chan error)

	go func() {
		var wstatus syscall.WaitStatus
		var rusage syscall.Rusage
		_, err := syscall.Wait4(pid, &wstatus, 0, &rusage)
		done <- err
		close(done)
	}()

	select {
	case err, _ := <-done:
		if err != nil {
			log.WithError(err).Warn("Failed to kill pid ", pid)
		}
	case <-time.After(superv.Timeout):
		log.Warn("Sending SIGKILL to ", pid)
		syscall.Kill(pid, unix.SIGKILL)
	}
}
