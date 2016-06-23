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

package config

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// BindStompFlags registers common stomp related flags
func BindStompFlags(cmd *cobra.Command) {
	cmd.Flags().String("Stomp", "localhost:61613", "Stomp host and port")
	cmd.Flags().Int("StompReconnectRetry", 5, "Maximum number of reconnect retries")
	cmd.Flags().Int("StompReconnectWait", 1, "Number of seconds to wait between reconnection attemps")
	cmd.Flags().String("StompLogin", "fts", "Stomp user")
	cmd.Flags().String("StompPasscode", "fts", "Stomp passcode")

	viper.BindPFlag("stomp", cmd.Flags().Lookup("Stomp"))
	viper.BindPFlag("stomp.reconnect.retry", cmd.Flags().Lookup("StompReconnectRetry"))
	viper.BindPFlag("stomp.reconnect.wait", cmd.Flags().Lookup("StompReconnectWait"))
	viper.BindPFlag("stomp.login", cmd.Flags().Lookup("StompLogin"))
	viper.BindPFlag("stomp.passcode", cmd.Flags().Lookup("StompPasscode"))
}
