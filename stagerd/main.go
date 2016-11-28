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
	"github.com/satori/go.uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gitlab.cern.ch/flutter/fts/config"
	"gitlab.cern.ch/flutter/fts/sink"
	"gitlab.cern.ch/flutter/stomp"
	"os"
	"time"
)

var stagerCmd = cobra.Command{
	Use:   "fts-stagerd",
	Short: "FTS Stager",
	Run: func(cmd *cobra.Command, args []string) {
		reconnectWait := viper.Get("stomp.reconnect.wait").(int)
		reconnectMaxRetries := viper.Get("stomp.reconnect.retry").(int)
		reconnectRetries := 0

		hostname, _ := os.Hostname()

		stompParams := stomp.ConnectionParameters{
			ClientID: "fts-stagerd-" + hostname,
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
		}

		if err := sink.Purge(stompParams, "Consumer.stager.fts.transfer", "stager-"+uuid.NewV4().String()); err != nil {
			log.Fatal(err)
		}
	},
}

func main() {
	// Config file
	configFile := stagerCmd.Flags().String("Config", "", "Use configuration from this file")

	// Stomp flags
	config.BindStompFlags(&stagerCmd)

	// Specific flags
	stagerCmd.Flags().String("Log", "", "Log file")
	stagerCmd.Flags().Bool("Debug", true, "Enable debugging")

	viper.BindPFlag("stagerd.log", stagerCmd.Flags().Lookup("Log"))
	viper.BindPFlag("stagerd.debug", stagerCmd.Flags().Lookup("Debug"))

	cobra.OnInitialize(func() {
		if *configFile != "" {
			config.ReadConfigFile(*configFile)
		}
		logFile := viper.Get("stagerd.log").(string)
		if logFile != "" {
			config.RedirectLog(logFile)
		}
		if viper.Get("stagerd.debug").(bool) {
			log.SetLevel(log.DebugLevel)
		}
	})

	if err := stagerCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
