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
	"gitlab.cern.ch/flutter/fts/util"
	"net/http"
)

var gatedCmd = &cobra.Command{
	Use:   "fts-gated",
	Short: "FTS Submission Gateway",
	Run: func(cmd *cobra.Command, args []string) {
		var err error

		// Instantiate store
		amqpAddr := viper.Get("amqp").(string)
		gateRPC, err := newRPC(amqpAddr)
		if err != nil {
			log.Fatal(err)
		}

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

	// Flags
	gatedCmd.Flags().String("Log", "", "Log file")
	gatedCmd.Flags().String("Listen", "localhost:42010", "Bind to this address")
	gatedCmd.Flags().String("Amqp", "amqp://guest:guest@localhost:5672/", "AMQP connect string")
	gatedCmd.Flags().Bool("Debug", true, "Enable debugging")

	// Bind flags to viper
	viper.BindPFlag("amqp", gatedCmd.Flags().Lookup("Amqp"))
	viper.BindPFlag("gated.log", gatedCmd.Flags().Lookup("Log"))
	viper.BindPFlag("gated.listen", gatedCmd.Flags().Lookup("Listen"))
	viper.BindPFlag("gated.debug", gatedCmd.Flags().Lookup("Debug"))

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
