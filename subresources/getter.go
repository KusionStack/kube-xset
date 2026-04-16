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

package subresources

import "kusionstack.io/kube-xset/api"

func GetSubresourcePvcAdapter(control api.XSetController) (adapter api.SubResourcePvcAdapter, enabled bool) {
	adapter, enabled = control.(api.SubResourcePvcAdapter)
	return adapter, enabled
}

// GetSubResourceAdapters returns subresource adapters if the controller implements SubResourceAdapterGetter.
func GetSubResourceAdapters(control api.XSetController) (adapters []api.SubResourceAdapter, enabled bool) {
	getter, ok := control.(api.SubResourceAdapterGetter)
	if !ok {
		return nil, false
	}
	return getter.GetSubResourceAdapters(), true
}

// BuildAdapters builds the adapter list with auto-bridge for legacy controllers.
// Priority:
// 1. If controller implements SubResourceAdapterGetter, use its adapters
// 2. Else if controller implements SubResourcePvcAdapter, auto-bridge to SubResourceControl
// 3. Else return nil (no subresource management)
func BuildAdapters(controller api.XSetController, labelAnnoMgr api.XSetLabelAnnotationManager) []api.SubResourceAdapter {
	// Priority 1: Controller provides its own adapters
	if getter, ok := controller.(api.SubResourceAdapterGetter); ok {
		return getter.GetSubResourceAdapters()
	}

	// Priority 2: Auto-bridge legacy SubResourcePvcAdapter
	if _, ok := controller.(api.SubResourcePvcAdapter); ok {
		return []api.SubResourceAdapter{
			NewPvcSubResourceAdapter(controller, labelAnnoMgr),
		}
	}

	// No subresource management
	return nil
}
