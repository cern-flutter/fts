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
	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gitlab.cern.ch/flutter/fts/config"
	"gitlab.cern.ch/flutter/stomp"
	"os"
	"os/exec"
	"time"
)

var workerCmd = &cobra.Command{
	Use:   "fts-workerd",
	Short: "FTS Worker",
	Run: func(cmd *cobra.Command, args []string) {
		var err error
		var w *Worker

		hostname, _ := os.Hostname()

		reconnectWait := viper.Get("stomp.reconnect.wait").(int)
		reconnectMaxRetries := viper.Get("stomp.reconnect.retry").(int)
		reconnectRetries := 0

		urlcopy, err := exec.LookPath(viper.Get("worker.urlcopy").(string))
		if err != nil {
			log.Fatal(err)
		}

		params := Params{
			Database:        viper.Get("worker.database").(string),
			URLCopyBin:      urlcopy,
			TransferLogPath: viper.Get("worker.transfers.logs").(string),
			DirQPath:        viper.Get("worker.dirq").(string),
			PidDBPath:       viper.Get("worker.piddb").(string),
			StompParams: stomp.ConnectionParameters{
				ClientID: "fts-workerd-" + hostname,
				Address:  viper.Get("stomp").(string),
				Login:    viper.Get("stomp.login").(string),
				Passcode: viper.Get("stomp.passcode").(string),
				ConnectionLost: func(b *stomp.Broker) {
					l := log.WithField("broker", b.RemoteAddr())
					if reconnectRetries >= reconnectMaxRetries {
						l.Panicf("Could not reconnect to the broker after %d attemps", reconnectRetries)
					}
					l.Warn("Lost connection with broker")
					if err := b.Reconnect(); err != nil {
						l.WithError(err).Errorf("Failed to reconnect, wait %d seconds", reconnectWait)
						time.Sleep(time.Duration(reconnectWait) * time.Second)
						reconnectRetries++
					} else {
						reconnectRetries = 0
					}
				},
			}}

		if w, err = NewWorker(params); err != nil {
			log.Fatal("Could not create a worker context: ", err)
		}
		defer w.Close()

		if err := w.Run(); err != nil {
			log.Fatal("Worker fatal error: ", err)
		}
	},
}

// Entry point
func main() {
	// Config file
	configFile := workerCmd.Flags().String("Config", "", "Use configuration from this file")

	// Stomp flags
	config.BindStompFlags(workerCmd)

	// Specific flags
	workerCmd.Flags().String("Log", "", "Log file")
	workerCmd.Flags().String("Database", "postgres://fts:fts@localhost:5432/fts?sslmode=disable", "X509 store rpc address")
	workerCmd.Flags().String("DirQ", "/var/lib/fts", "Base directory for dirq messages")
	workerCmd.Flags().String("PidDB", "/var/lib/fts/pid.db", "PID database")
	workerCmd.Flags().String("UrlCopy", "url-copy", "url-copy command")
	workerCmd.Flags().String("TransfersLogDir", "/var/log/fts/transfers", "Transfer logs base dir")
	workerCmd.Flags().Bool("Debug", true, "Enable debugging")

	viper.BindPFlag("worker.log", workerCmd.Flags().Lookup("Log"))
	viper.BindPFlag("worker.database", workerCmd.Flags().Lookup("Database"))
	viper.BindPFlag("worker.dirq", workerCmd.Flags().Lookup("DirQ"))
	viper.BindPFlag("worker.piddb", workerCmd.Flags().Lookup("PidDB"))
	viper.BindPFlag("worker.urlcopy", workerCmd.Flags().Lookup("UrlCopy"))
	viper.BindPFlag("worker.transfers.logs", workerCmd.Flags().Lookup("TransfersLogDir"))
	viper.BindPFlag("worker.debug", workerCmd.Flags().Lookup("Debug"))

	cobra.OnInitialize(func() {
		if *configFile != "" {
			config.ReadConfigFile(*configFile)
		}
		logFile := viper.Get("worker.log").(string)
		if logFile != "" {
			config.RedirectLog(logFile)
		}
		if viper.Get("worker.debug").(bool) {
			log.SetLevel(log.DebugLevel)
		}
	})

	if err := workerCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
