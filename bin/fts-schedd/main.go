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
	"gitlab.cern.ch/flutter/fts/scheduler"
	"gitlab.cern.ch/flutter/fts/util"
	"gitlab.cern.ch/flutter/stomp"
	"time"
)

var scheddCmd = &cobra.Command{
	Use:   "fts-schedd",
	Short: "FTS Scheduler",
	Run: func(cmd *cobra.Command, args []string) {
		reconnectWait := viper.Get("stomp.reconnect.wait").(int)
		reconnectMaxRetries := viper.Get("stomp.reconnect.retry").(int)
		reconnectRetries := 0

		sched, err := scheduler.New(stomp.ConnectionParameters{
			ClientId: "fts-schedd-" + util.Hostname(),
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
		}, viper.Get("schedd.dirq").(string))
		if err != nil {
			log.Fatal(err)
		}
		defer sched.Close()
		consumerErrors := sched.GoConsumer()
		producerErrors := sched.GoProducer()
		log.Info("All subservices started")
		for {
			select {
			case e := <-consumerErrors:
				log.Fatal("Scheduler consumer failed with ", e)
			case e := <-producerErrors:
				log.Fatal("Scheduler producer failed with ", e)
			}
		}
	},
}

// Entry point
func main() {
	// Config file
	configFile := scheddCmd.Flags().String("Config", "", "Use configuration from this file")
	scheddCmd.Flags().String("EchelonDir", "/var/lib/fts/echelon", "Echelon persistency dir")

	// Stomp flags
	config.BindStompFlags(scheddCmd)

	// Specific flags
	scheddCmd.Flags().String("Log", "", "Log file")
	scheddCmd.Flags().Bool("Debug", true, "Enable debugging")
	viper.BindPFlag("schedd.log", scheddCmd.Flags().Lookup("Log"))
	viper.BindPFlag("schedd.debug", scheddCmd.Flags().Lookup("Debug"))
	viper.BindPFlag("schedd.dirq", scheddCmd.Flags().Lookup("EchelonDir"))

	cobra.OnInitialize(func() {
		if *configFile != "" {
			util.ReadConfigFile(*configFile)
		}
		logFile := viper.Get("schedd.log").(string)
		if logFile != "" {
			util.RedirectLog(logFile)
		}
		if viper.Get("schedd.debug").(bool) {
			log.SetLevel(log.DebugLevel)
		}
	})

	if err := scheddCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
