/*
 * Copyright 2024-2026 KusionStack Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package features

import "testing"

func TestConcurrentPatchRolloutDefaultsOnForExistingObjects(t *testing.T) {
	if !DefaultFeatureGate.Enabled(ConcurrentPatchRollout) {
		t.Fatal("ConcurrentPatchRollout must default on so upgrades do not freeze existing journals")
	}
}
