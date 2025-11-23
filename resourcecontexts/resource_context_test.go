/*
Copyright 2023-2025 The KusionStack Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resourcecontexts

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/pointer"
	"kusionstack.io/kube-utils/controller/expectations"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-xset/api"
)

func TestRealResourceContextControl_fulfillOwnedIDs(t *testing.T) {
	type fields struct {
		Client                 client.Client
		EventRecorder          record.EventRecorder
		xsetController         api.XSetController
		resourceContextAdapter api.ResourceContextAdapter
		resourceContextKeys    map[api.ResourceContextKeyEnum]string
		resourceContextGVK     schema.GroupVersionKind
		cacheExpectations      expectations.CacheExpectationsInterface
		xsetLabelManager       api.XSetLabelAnnotationManager
	}
	type args struct {
		ownedIDs        map[int]*api.ContextDetail
		existingIDs     map[int]*api.ContextDetail
		unRecordIDs     map[int]string
		replicas        int
		ownerName       string
		spec            *api.XSetSpec
		currentRevision string
		updatedRevision string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[int]*api.ContextDetail
	}{
		{
			name: "want 5, existing [0], alloc [1,2,3,4]",
			args: args{
				ownedIDs: map[int]*api.ContextDetail{
					0: {
						ID:   0,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
				},
				existingIDs: map[int]*api.ContextDetail{
					0: {
						ID:   0,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
				},
				unRecordIDs:     map[int]string{},
				ownerName:       "foo",
				spec:            &api.XSetSpec{},
				replicas:        5,
				currentRevision: "defaultRv",
				updatedRevision: "defaultRv",
			},
			fields: fields{
				Client:                 nil,
				EventRecorder:          nil,
				xsetController:         nil,
				resourceContextAdapter: nil,
				resourceContextKeys:    defaultResourceContextKeys,
				resourceContextGVK:     schema.GroupVersionKind{},
				cacheExpectations:      nil,
				xsetLabelManager:       nil,
			},
			want: map[int]*api.ContextDetail{
				0: {
					ID:   0,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
				1: {
					ID:   1,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
				2: {
					ID:   2,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
				3: {
					ID:   3,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
				4: {
					ID:   4,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
			},
		},
		{
			name: "want 3, existing [0,1], unrecorded [3], alloc [3]",
			args: args{
				ownedIDs: map[int]*api.ContextDetail{
					0: {
						ID:   0,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
					1: {
						ID:   1,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
				},
				existingIDs: map[int]*api.ContextDetail{
					0: {
						ID:   0,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
					1: {
						ID:   1,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
				},
				unRecordIDs:     map[int]string{3: "defaultRv"},
				replicas:        2,
				ownerName:       "foo",
				spec:            &api.XSetSpec{},
				currentRevision: "defaultRv",
				updatedRevision: "defaultRv",
			},
			fields: fields{
				Client:                 nil,
				EventRecorder:          nil,
				xsetController:         nil,
				resourceContextAdapter: nil,
				resourceContextKeys:    defaultResourceContextKeys,
				resourceContextGVK:     schema.GroupVersionKind{},
				cacheExpectations:      nil,
				xsetLabelManager:       nil,
			},
			want: map[int]*api.ContextDetail{
				0: {
					ID:   0,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
				1: {
					ID:   1,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
				3: {
					ID:   3,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
			},
		},
		{
			name: "want 4, existing [0,1], unrecorded [3], alloc [2,3]",
			args: args{
				ownedIDs: map[int]*api.ContextDetail{
					0: {
						ID:   0,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
					1: {
						ID:   1,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
				},
				existingIDs: map[int]*api.ContextDetail{
					0: {
						ID:   0,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
					1: {
						ID:   1,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
				},
				unRecordIDs:     map[int]string{3: "defaultRv"},
				replicas:        4,
				ownerName:       "foo",
				spec:            &api.XSetSpec{},
				currentRevision: "defaultRv",
				updatedRevision: "defaultRv",
			},
			fields: fields{
				Client:                 nil,
				EventRecorder:          nil,
				xsetController:         nil,
				resourceContextAdapter: nil,
				resourceContextKeys:    defaultResourceContextKeys,
				resourceContextGVK:     schema.GroupVersionKind{},
				cacheExpectations:      nil,
				xsetLabelManager:       nil,
			},
			want: map[int]*api.ContextDetail{
				0: {
					ID:   0,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
				1: {
					ID:   1,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
				2: {
					ID:   2,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
				3: {
					ID:   3,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RealResourceContextControl{
				Client:                 tt.fields.Client,
				EventRecorder:          tt.fields.EventRecorder,
				xsetController:         tt.fields.xsetController,
				resourceContextAdapter: tt.fields.resourceContextAdapter,
				resourceContextKeys:    tt.fields.resourceContextKeys,
				resourceContextGVK:     tt.fields.resourceContextGVK,
				cacheExpectations:      tt.fields.cacheExpectations,
				xsetLabelManager:       tt.fields.xsetLabelManager,
			}
			if got := r.doAllocateID(tt.args.ownedIDs, tt.args.existingIDs, tt.args.unRecordIDs, tt.args.replicas, tt.args.ownerName, tt.args.spec, tt.args.currentRevision, tt.args.updatedRevision); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRealResourceContextControl_DecideContextsRevisionBeforeCreate(t *testing.T) {
	type fields struct {
		Client                 client.Client
		EventRecorder          record.EventRecorder
		xsetController         api.XSetController
		resourceContextAdapter api.ResourceContextAdapter
		resourceContextKeys    map[api.ResourceContextKeyEnum]string
		resourceContextGVK     schema.GroupVersionKind
		cacheExpectations      expectations.CacheExpectationsInterface
		xsetLabelManager       api.XSetLabelAnnotationManager
	}
	type args struct {
		ownedIDs        map[int]*api.ContextDetail
		newIDs          map[int]*api.ContextDetail
		spec            *api.XSetSpec
		currentRevision string
		updatedRevision string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[int]*api.ContextDetail
	}{
		{
			name: "ownedIDs[0: oldRevision, 1: oldRevision], replicas: 4, byLabel, want newIDs[2: oldRevision, 3: oldRevision]",
			fields: fields{
				Client:                 nil,
				EventRecorder:          nil,
				xsetController:         nil,
				resourceContextAdapter: nil,
				resourceContextKeys:    defaultResourceContextKeys,
				resourceContextGVK:     schema.GroupVersionKind{},
				cacheExpectations:      nil,
				xsetLabelManager:       nil,
			},
			args: args{
				ownedIDs: map[int]*api.ContextDetail{
					0: {
						ID:   0,
						Data: map[string]string{"Owner": "foo", "Revision": "oldRevision"},
					},
					1: {
						ID:   1,
						Data: map[string]string{"Owner": "foo", "Revision": "oldRevision"},
					},
				},
				newIDs: map[int]*api.ContextDetail{
					2: {
						ID:   2,
						Data: map[string]string{"Owner": "foo"},
					},
					3: {
						ID:   3,
						Data: map[string]string{"Owner": "foo"},
					},
				},
				spec: &api.XSetSpec{
					Replicas: pointer.Int32(4),
					UpdateStrategy: api.UpdateStrategy{
						RollingUpdate: &api.RollingUpdateStrategy{
							ByLabel: &api.ByLabel{},
						},
					},
				},
				currentRevision: "oldRevision",
				updatedRevision: "newRevision",
			},
			want: map[int]*api.ContextDetail{
				2: {
					ID:   2,
					Data: map[string]string{"Owner": "foo", "Revision": "oldRevision"},
				},
				3: {
					ID:   3,
					Data: map[string]string{"Owner": "foo", "Revision": "oldRevision"},
				},
			},
		},
		{
			name: "ownedIDs[0: oldRevision, 1: oldRevision], replicas: 4, partition=nil, want newIDs[2: newRevision, 3: newRevision]",
			fields: fields{
				Client:                 nil,
				EventRecorder:          nil,
				xsetController:         nil,
				resourceContextAdapter: nil,
				resourceContextKeys:    defaultResourceContextKeys,
				resourceContextGVK:     schema.GroupVersionKind{},
				cacheExpectations:      nil,
				xsetLabelManager:       nil,
			},
			args: args{
				ownedIDs: map[int]*api.ContextDetail{
					0: {
						ID:   0,
						Data: map[string]string{"Owner": "foo", "Revision": "oldRevision"},
					},
					1: {
						ID:   1,
						Data: map[string]string{"Owner": "foo", "Revision": "oldRevision"},
					},
				},
				newIDs: map[int]*api.ContextDetail{
					2: {
						ID:   2,
						Data: map[string]string{"Owner": "foo"},
					},
					3: {
						ID:   3,
						Data: map[string]string{"Owner": "foo"},
					},
				},
				spec: &api.XSetSpec{
					Replicas: pointer.Int32(4),
					UpdateStrategy: api.UpdateStrategy{
						RollingUpdate: nil,
					},
				},
				currentRevision: "oldRevision",
				updatedRevision: "newRevision",
			},
			want: map[int]*api.ContextDetail{
				2: {
					ID:   2,
					Data: map[string]string{"Owner": "foo", "Revision": "newRevision"},
				},
				3: {
					ID:   3,
					Data: map[string]string{"Owner": "foo", "Revision": "newRevision"},
				},
			},
		},
		{
			name: "ownedIDs[0: oldRevision, 1: oldRevision], replicas: 4, partition: 2, want newIDs[2: updatedRevision, 3: updatedRevision]",
			fields: fields{
				Client:                 nil,
				EventRecorder:          nil,
				xsetController:         nil,
				resourceContextAdapter: nil,
				resourceContextKeys:    defaultResourceContextKeys,
				resourceContextGVK:     schema.GroupVersionKind{},
				cacheExpectations:      nil,
				xsetLabelManager:       nil,
			},
			args: args{
				ownedIDs: map[int]*api.ContextDetail{
					0: {
						ID:   0,
						Data: map[string]string{"Owner": "foo", "Revision": "oldRevision"},
					},
					1: {
						ID:   1,
						Data: map[string]string{"Owner": "foo", "Revision": "oldRevision"},
					},
				},
				newIDs: map[int]*api.ContextDetail{
					2: {
						ID:   2,
						Data: map[string]string{"Owner": "foo"},
					},
					3: {
						ID:   3,
						Data: map[string]string{"Owner": "foo"},
					},
				},
				spec: &api.XSetSpec{
					Replicas: pointer.Int32(4),
					UpdateStrategy: api.UpdateStrategy{
						RollingUpdate: &api.RollingUpdateStrategy{
							ByPartition: &api.ByPartition{
								Partition: pointer.Int32(2),
							},
						},
					},
				},
				currentRevision: "oldRevision",
				updatedRevision: "newRevision",
			},
			want: map[int]*api.ContextDetail{
				2: {
					ID:   2,
					Data: map[string]string{"Owner": "foo", "Revision": "newRevision"},
				},
				3: {
					ID:   3,
					Data: map[string]string{"Owner": "foo", "Revision": "newRevision"},
				},
			},
		},
		{
			name: "ownedIDs[0: oldRevision, 1: oldRevision], replicas: 5, partition: 4, want newIDs[2: oldRevision, 3: oldRevision, 4: newRevision]",
			fields: fields{
				Client:                 nil,
				EventRecorder:          nil,
				xsetController:         nil,
				resourceContextAdapter: nil,
				resourceContextKeys:    defaultResourceContextKeys,
				resourceContextGVK:     schema.GroupVersionKind{},
				cacheExpectations:      nil,
				xsetLabelManager:       nil,
			},
			args: args{
				ownedIDs: map[int]*api.ContextDetail{
					0: {
						ID:   0,
						Data: map[string]string{"Owner": "foo", "Revision": "oldRevision"},
					},
					1: {
						ID:   1,
						Data: map[string]string{"Owner": "foo", "Revision": "oldRevision"},
					},
				},
				newIDs: map[int]*api.ContextDetail{
					2: {
						ID:   2,
						Data: map[string]string{"Owner": "foo"},
					},
					3: {
						ID:   3,
						Data: map[string]string{"Owner": "foo"},
					},
					4: {
						ID:   4,
						Data: map[string]string{"Owner": "foo"},
					},
				},
				spec: &api.XSetSpec{
					Replicas: pointer.Int32(5),
					UpdateStrategy: api.UpdateStrategy{
						RollingUpdate: &api.RollingUpdateStrategy{
							ByPartition: &api.ByPartition{
								Partition: pointer.Int32(4),
							},
						},
					},
				},
				currentRevision: "oldRevision",
				updatedRevision: "newRevision",
			},
			want: map[int]*api.ContextDetail{
				2: {
					ID:   2,
					Data: map[string]string{"Owner": "foo", "Revision": "oldRevision"},
				},
				3: {
					ID:   3,
					Data: map[string]string{"Owner": "foo", "Revision": "oldRevision"},
				},
				4: {
					ID:   4,
					Data: map[string]string{"Owner": "foo", "Revision": "newRevision"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RealResourceContextControl{
				Client:                 tt.fields.Client,
				EventRecorder:          tt.fields.EventRecorder,
				xsetController:         tt.fields.xsetController,
				resourceContextAdapter: tt.fields.resourceContextAdapter,
				resourceContextKeys:    tt.fields.resourceContextKeys,
				resourceContextGVK:     tt.fields.resourceContextGVK,
				cacheExpectations:      tt.fields.cacheExpectations,
				xsetLabelManager:       tt.fields.xsetLabelManager,
			}
			r.DecideContextsRevisionBeforeCreate(tt.args.ownedIDs, tt.args.newIDs, tt.args.spec, tt.args.currentRevision, tt.args.updatedRevision)
			if !reflect.DeepEqual(tt.args.newIDs, tt.want) {
				t.Errorf("got %v, want %v", tt.args.newIDs, tt.want)
			}
		})
	}
}
