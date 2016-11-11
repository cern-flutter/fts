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
	"github.com/gorilla/mux"
	"github.com/gorilla/rpc/v2"
	json "github.com/gorilla/rpc/v2/json2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gitlab.cern.ch/flutter/fts/config"
	"gitlab.cern.ch/flutter/fts/util"
	"gitlab.cern.ch/flutter/stomp"
	"net/http"
	"time"
)

var gatedCmd = &cobra.Command{
	Use:   "fts-gated",
	Short: "FTS Submission Gateway",
	Run: func(cmd *cobra.Command, args []string) {
		var err error

		// Instantiate store
		reconnectWait := viper.Get("stomp.reconnect.wait").(int)
		reconnectMaxRetries := viper.Get("stomp.reconnect.retry").(int)
		reconnectRetries := 0

		gateRPC, err := newRPC(stomp.ConnectionParameters{
			ClientID: "fts-gated-" + util.Hostname(),
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
		})
		if err != nil {
			log.Fatal(err)
		}
		defer gateRPC.close()

		// Create jsonrpc server
		server := rpc.NewServer()
		server.RegisterCodec(json.NewCodec(), "application/json")
		server.RegisterCodec(json.NewCodec(), "application/json;charset=UTF-8")
		server.RegisterCodec(json.NewCodec(), "application/json-rpc")
		if err = server.RegisterService(gateRPC, "Gate"); err != nil {
			log.Fatal(err)
		}

		router := mux.NewRouter()
		router.Handle("/rpc", server)

		// Run jsonrpc server
		listenAddr := viper.Get("gated.listen").(string)
		log.Info("Listening on ", listenAddr)
		if err = http.ListenAndServe(listenAddr, router); err != nil {
			log.Fatal(err)
		}
	},
}

// Entry point
func main() {
	// Config file
	configFile := gatedCmd.Flags().String("Config", "", "Use configuration from this file")

	// Common configuration
	config.BindStompFlags(gatedCmd)

	// Specific configuration
	gatedCmd.Flags().String("Log", "", "Log file")
	gatedCmd.Flags().Bool("Debug", true, "Enable debugging")
	gatedCmd.Flags().String("Listen", "localhost:42010", "Bind to this address")

	viper.BindPFlag("gated.log", gatedCmd.Flags().Lookup("Log"))
	viper.BindPFlag("gated.debug", gatedCmd.Flags().Lookup("Debug"))
	viper.BindPFlag("gated.listen", gatedCmd.Flags().Lookup("Listen"))

	cobra.OnInitialize(func() {
		if *configFile != "" {
			util.ReadConfigFile(*configFile)
		}
		logFile := viper.Get("gated.log").(string)
		if logFile != "" {
			util.RedirectLog(logFile)
		}
		if viper.Get("gated.debug").(bool) {
			log.SetLevel(log.DebugLevel)
		}
	})

	if err := gatedCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
