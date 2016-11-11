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
	"gitlab.cern.ch/flutter/fts/util"
	"gitlab.cern.ch/flutter/stomp"
	"time"
)

var storeCmd = cobra.Command{
	Use:   "fts-stored",
	Short: "FTS Store",
	Run: func(cmd *cobra.Command, args []string) {
		reconnectWait := viper.Get("stomp.reconnect.wait").(int)
		reconnectMaxRetries := viper.Get("stomp.reconnect.retry").(int)
		reconnectRetries := 0

		stompParams := stomp.ConnectionParameters{
			ClientID: "fts-stored-" + util.Hostname(),
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

		if err := sink.Purge(stompParams, "Consumer.db.fts.transfer", "stored-"+uuid.NewV4().String()); err != nil {
			log.Fatal(err)
		}
	},
}

func main() {
	// Config file
	configFile := storeCmd.Flags().String("Config", "", "Use configuration from this file")

	// Stomp flags
	config.BindStompFlags(&storeCmd)

	// Specific flags
	storeCmd.Flags().String("Log", "", "Log file")
	storeCmd.Flags().Bool("Debug", true, "Enable debugging")

	viper.BindPFlag("stored.log", storeCmd.Flags().Lookup("Log"))
	viper.BindPFlag("stored.debug", storeCmd.Flags().Lookup("Debug"))

	cobra.OnInitialize(func() {
		if *configFile != "" {
			util.ReadConfigFile(*configFile)
		}
		logFile := viper.Get("stored.log").(string)
		if logFile != "" {
			util.RedirectLog(logFile)
		}
		if viper.Get("stored.debug").(bool) {
			log.SetLevel(log.DebugLevel)
		}
	})

	if err := storeCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
