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

package xcontrol

import (
	"fmt"
	"strconv"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-xset/api"
)

func GetInstanceID(xsetLabelAnnoMgr api.XSetLabelAnnotationManager, target client.Object) (int, error) {
	if target.GetLabels() == nil {
		return -1, fmt.Errorf("no labels found for instance ID")
	}

	instanceIdLabelKey := xsetLabelAnnoMgr.Value(api.XInstanceIdLabelKey)
	val, exist := target.GetLabels()[instanceIdLabelKey]
	if !exist {
		return -1, fmt.Errorf("failed to find instance ID label %s", instanceIdLabelKey)
	}

	id, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		// ignore invalid target instance ID
		return -1, fmt.Errorf("failed to parse instance ID with value %s: %w", val, err)
	}

	return int(id), nil
}
