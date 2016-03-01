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
	log "github.com/Sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

// signalHandler listen for signals that are triggered either by a fatal error inside
// the code (i.e. SIGSEGV), or cancellation signals coming from FTS (i.e. SIGTERM).
// For fatal error signals, it will force-quit after trying to send the terminal messages.
func signalHandler(copy *UrlCopy) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGABRT, syscall.SIGSEGV, syscall.SIGILL, syscall.SIGFPE,
		syscall.SIGBUS, syscall.SIGTRAP, syscall.SIGSYS, syscall.SIGINT, syscall.SIGTERM)

	for signum := range c {
		log.Warning("Received signal ", signum)
		switch signum {
		case syscall.SIGINT, syscall.SIGTERM:
			copy.Cancel()
		default:
			copy.Panic("Transfer process died with: %d", signum)
			log.Panic("Transfer process died with: ", signum)
		}
	}
}

// Entry point
func main() {
	if os.Getuid() == 0 || os.Getgid() == 0 {
		log.Warning("Running as root! This is not recommended.")
	}

	flag.Parse()
	setupLogging()

	if flag.NArg() == 0 {
		log.Panic("Missing task file.")
	}

	copy := NewUrlCopy(flag.Arg(0))
	go signalHandler(copy)
	copy.Run()
}
