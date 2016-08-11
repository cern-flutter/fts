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
	"gitlab.cern.ch/flutter/fts/credentials/x509"
	"gitlab.cern.ch/flutter/fts/util"
	"net/http"
)

var x509Cmd = &cobra.Command{
	Use:   "fts-x509d",
	Short: "FTS X509 Store",
	Run: func(cmd *cobra.Command, args []string) {
		var err error

		// Instantiate store
		dbAddr := viper.Get("x509.db").(string)
		var x509rpc X509RPC
		if x509rpc.store, err = x509.NewStore(dbAddr); err != nil {
			log.Fatal("Could not instantiate the service: ", err)
		}
		defer x509rpc.store.Close()

		// Create jsonrpc server
		server := rpc.NewServer()
		server.RegisterCodec(json.NewCodec(), "application/json")
		server.RegisterCodec(json.NewCodec(), "application/json;charset=UTF-8")
		server.RegisterCodec(json.NewCodec(), "application/json-rpc")
		if err = server.RegisterService(&x509rpc, "X509"); err != nil {
			log.Fatal(err)
		}

		router := mux.NewRouter()
		router.Handle("/rpc", server)

		// Run jsonrpc server
		listenAddr := viper.Get("x509.listen").(string)
		log.Info("Listening on ", listenAddr)
		if err = http.ListenAndServe(listenAddr, router); err != nil {
			log.Fatal(err)
		}
	},
}

// Entry point
func main() {
	// Config file
	configFile := x509Cmd.Flags().String("Config", "", "Use configuration from this file")

	// Flags
	x509Cmd.Flags().String("Log", "", "Log file")
	x509Cmd.Flags().String("Listen", "localhost:42001", "Bind to this address")
	x509Cmd.Flags().String("Database", "dbname=fts user=fts password=fts host=localhost sslmode=disable",
		"Database connection string")
	x509Cmd.Flags().Bool("Debug", true, "Enable debugging")

	// Bind flags to viper
	viper.BindPFlag("x509.log", x509Cmd.Flags().Lookup("Log"))
	viper.BindPFlag("x509.listen", x509Cmd.Flags().Lookup("Listen"))
	viper.BindPFlag("x509.db", x509Cmd.Flags().Lookup("Database"))
	viper.BindPFlag("x509.debug", x509Cmd.Flags().Lookup("Debug"))

	cobra.OnInitialize(func() {
		if *configFile != "" {
			util.ReadConfigFile(*configFile)
		}
		logFile := viper.Get("x509.log").(string)
		if logFile != "" {
			util.RedirectLog(logFile)
		}
		if viper.Get("x509.debug").(bool) {
			log.SetLevel(log.DebugLevel)
		}
	})

	if err := x509Cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
