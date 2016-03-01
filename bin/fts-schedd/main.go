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
	"gitlab.cern.ch/flutter/fts/scheduler"
	"gitlab.cern.ch/flutter/fts/util"
)

var scheddCmd = &cobra.Command{
	Use:   "fts-schedd",
	Short: "FTS Scheduler",
	Run: func(cmd *cobra.Command, args []string) {
		amqpAddress := viper.Get("amqp").(string)
		sched, err := scheduler.New(amqpAddress)
		if err != nil {
			log.Fatal(err)
		}
		scheddErrors := sched.Go()
		log.Info("All subservices started")
		for {
			select {
			case e := <-scheddErrors:
				log.Fatal("Scheduler service failed with ", e)
			}
		}
	},
}

// Entry point
func main() {
	// Config file
	configFile := scheddCmd.Flags().String("Config", "", "Use configuration from this file")

	// Flags
	scheddCmd.Flags().String("Amqp", "amqp://guest:guest@localhost:5672/", "AMQP connect string")
	scheddCmd.Flags().String("Log", "", "Log file")
	scheddCmd.Flags().Bool("Debug", true, "Enable debugging")

	// Bind flags to viper
	viper.BindPFlag("amqp", scheddCmd.Flags().Lookup("Amqp"))
	viper.BindPFlag("schedd.log", scheddCmd.Flags().Lookup("Log"))
	viper.BindPFlag("schedd.debug", scheddCmd.Flags().Lookup("Debug"))

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
