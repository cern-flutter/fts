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
	"gitlab.cern.ch/flutter/fts/messages"
	"math"
	"syscall"
	"time"
)

// IsRecoverable returns true if the error code/scope is considered an error
// caused by overload or transient errors. It returns false if the error would happen again
// if retried.
func IsRecoverable(scope messages.TransferError_Scope, code syscall.Errno) bool {
	if syscall.Errno(code) == syscall.ETIMEDOUT {
		return true
	}

	if syscall.Errno(code) == syscall.ECANCELED {
		return false
	}

	switch scope {
	case messages.TransferError_SOURCE:
		switch syscall.Errno(code) {
		case syscall.ENOENT,
			syscall.EPERM,
			syscall.EACCES,
			syscall.EISDIR,
			syscall.ENAMETOOLONG,
			syscall.E2BIG,
			syscall.ENOTDIR,
			syscall.EPROTONOSUPPORT:
			return false
		}
	case messages.TransferError_DESTINATION:
		switch syscall.Errno(code) {
		case syscall.EPERM,
			syscall.EACCES,
			syscall.EISDIR,
			syscall.ENAMETOOLONG,
			syscall.E2BIG,
			syscall.EPROTONOSUPPORT:
			return false
		}
	default:
		switch syscall.Errno(code) {
		case syscall.ENOSPC,
			syscall.EPERM,
			syscall.EACCES,
			syscall.EEXIST,
			syscall.EFBIG,
			syscall.EROFS,
			syscall.ENAMETOOLONG,
			syscall.EPROTONOSUPPORT:
			return false
		}
	}

	return true
}

// AdjustTimeoutBasedOnSize calculates the appropriated timeout depending on the file size.
func AdjustTimeoutBasedOnSize(size int64) time.Duration {
	var MB = float64(1024 * 1024)

	addSecsPerMb := 2 * time.Second
	timeout := int64(math.Ceil(float64(size)/MB)) * int64(addSecsPerMb)
	return time.Duration(timeout)
}
