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

package util

import (
	log "github.com/Sirupsen/logrus"
	"github.com/Sirupsen/logrus/formatters/logstash"
	"github.com/spf13/viper"
	"os"
)

// ReadConfigFile reads the configuration file passed as parameter, aborts on error
func ReadConfigFile(configFile string) {
	if f, err := os.Open(configFile); err != nil {
		log.Fatal(err)
	} else {
		defer f.Close()
		viper.SetConfigType("yaml")
		if err = viper.ReadConfig(f); err != nil {
			log.Fatal(err)
		}
		log.Info("Read configuration from ", configFile)
	}
}

// RedirectLog redirects logrus to the output file, aborts on error
func RedirectLog(logFile string) {
	if f, err := os.OpenFile(logFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660); err != nil {
		log.Panic(err)
	} else {
		log.SetOutput(f)
		log.SetFormatter(&logstash.LogstashFormatter{})
	}
}

// Hostname returns the machine name
func Hostname() string {
	if host, err := os.Hostname(); err == nil {
		return host
	}
	return "Unknown"
}
