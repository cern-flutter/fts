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

package scheduler

type (
	SchedInfoProvider struct {
	}
)

func (info *SchedInfoProvider) GetWeight(route []string) float32 {
	// TODO: Access config
	return 1.0
}

func (info *SchedInfoProvider) GetAvailableSlots(route []string) (int, error) {
	// TODO: Keep accounting
	return 1, nil
}

func (info *SchedInfoProvider) ConsumeSlot(route []string) error {
	// TODO: Keep accounting
	return nil
}
