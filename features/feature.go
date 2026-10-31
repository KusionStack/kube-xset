/*
 * Copyright 2024-2025 KusionStack Authors.
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

import (
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/component-base/featuregate"
)

const (
	// ReclaimScaleStrategy enables reclaim of xset.spec.scaleStrategy.targetToDelete
	ReclaimScaleStrategy featuregate.Feature = "ReclaimScaleStrategy"
	// ConcurrentPatchRollout enables independently planned patch revisions. It
	// defaults on for compatibility with objects that already use the mode, and
	// can be disabled as an emergency freeze without changing their journal.
	ConcurrentPatchRollout featuregate.Feature = "ConcurrentPatchRollout"
)

func init() {
	runtime.Must(DefaultMutableFeatureGate.Add(defaultKubernetesFeatureGates))
}

// defaultKubernetesFeatureGates consists of all known Kubernetes-specific feature keys.
// To add a new feature, define a key for it above and add it here. The features will be
// available throughout Kubernetes binaries.
var defaultKubernetesFeatureGates = map[featuregate.Feature]featuregate.FeatureSpec{
	ReclaimScaleStrategy:   {Default: false, PreRelease: featuregate.Alpha},
	ConcurrentPatchRollout: {Default: true, PreRelease: featuregate.Beta},
}
