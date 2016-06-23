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
	"gitlab.cern.ch/flutter/fts/util"
	"gitlab.cern.ch/flutter/fts/worker"
	"gitlab.cern.ch/flutter/stomp"
	"time"
)

var workerCmd = &cobra.Command{
	Use:   "fts-workerd",
	Short: "FTS Worker",
	Run: func(cmd *cobra.Command, args []string) {
		var err error
		var context *worker.Context

		reconnectWait := viper.Get("stomp.reconnect.wait").(int)
		reconnectMaxRetries := viper.Get("stomp.reconnect.retry").(int)
		reconnectRetries := 0

		params := worker.Params{
			X509Address:     viper.Get("worker.x509d").(string),
			URLCopyBin:      viper.Get("worker.urlcopy").(string),
			TransferLogPath: viper.Get("worker.transfers.logs").(string),
			DirQPath:        viper.Get("worker.dirq").(string),
			StompParams: stomp.ConnectionParameters{
				ClientId: "fts-workerd-" + util.Hostname(),
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

		if context, err = worker.New(params); err != nil {
			log.Fatal("Could not create a worker context: ", err)
		}
		defer context.Close()

		runner := (&worker.Runner{Context: context}).Go()
		killer := (&worker.Killer{Context: context}).Go()
		forwarder := (&worker.Forwarder{Context: context}).Go()

		log.Info("All subservices started")
		for {
			select {
			case e := <-runner:
				log.Fatal("Runner service failed with ", e)
			case e := <-killer:
				log.Error("Killer service failed with ", e)
			case e := <-forwarder:
				log.Fatal("Forwarder service failed with ", e)
			}
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
	workerCmd.Flags().String("X509d", "http://localhost:42001/rpc", "X509 store rpc address")
	workerCmd.Flags().String("DirQ", "/var/lib/fts", "Base directory for dirq messages")
	workerCmd.Flags().String("UrlCopy", "url-copy", "url-copy command")
	workerCmd.Flags().String("TransfersLogDir", "/var/log/fts/transfers", "Transfer logs base dir")
	workerCmd.Flags().Bool("Debug", true, "Enable debugging")

	viper.BindPFlag("worker.log", workerCmd.Flags().Lookup("Log"))
	viper.BindPFlag("worker.x509d", workerCmd.Flags().Lookup("X509d"))
	viper.BindPFlag("worker.dirq", workerCmd.Flags().Lookup("DirQ"))
	viper.BindPFlag("worker.urlcopy", workerCmd.Flags().Lookup("UrlCopy"))
	viper.BindPFlag("worker.transfers.logs", workerCmd.Flags().Lookup("TransfersLogDir"))
	viper.BindPFlag("worker.debug", workerCmd.Flags().Lookup("Debug"))

	cobra.OnInitialize(func() {
		if *configFile != "" {
			util.ReadConfigFile(*configFile)
		}
		logFile := viper.Get("worker.log").(string)
		if logFile != "" {
			util.RedirectLog(logFile)
		}
		if viper.Get("worker.debug").(bool) {
			log.SetLevel(log.DebugLevel)
		}
	})

	if err := workerCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
