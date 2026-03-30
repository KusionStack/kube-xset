# kube-xset Generic SubResource Support Design

**Date:** 2026-03-30
**Author:** Claude
**Status:** Draft

## Overview

This design proposes adding generic subresource support to kube-xset, enabling XSetControllers to manage multiple subresource types (PVC, Service, etc.) beyond the current PVC-only support.

### Goals

1. **Generic SubResource Adapter** - Support multiple subresource types with a clean abstraction
2. **Name Truncation** - Automatically truncate resource names exceeding Kubernetes limits (63 chars for DNS labels)
3. **Label Value Handling** - Truncate label values with hash suffix for uniqueness
4. **Code Optimization** - Abstract PVC-specific code into reusable patterns
5. **Backward Compatibility** - Keep existing `SubResourcePvcAdapter` interface working

### Non-Goals

- Changing existing PVC behavior for controllers using `SubResourcePvcAdapter`
- Supporting stateful subresource ordering (that's ResourceContext's job)

## Design

### 1. Core Interfaces

#### SubResourceAdapter Interface

```go
// api/subresource_types.go

package api

import (
    "context"
    "sigs.k8s.io/controller-runtime/pkg/client"
    "k8s.io/apimachinery/pkg/runtime/schema"
)

// SubResourceAdapter is a generic interface for XSet subresources.
// Each subresource type (PVC, Service, ConfigMap, etc.) implements this interface.
type SubResourceAdapter interface {
    // Meta returns the GroupVersionKind for this subresource type
    Meta() schema.GroupVersionKind

    // GetTemplates returns subresource templates from XSet spec
    GetTemplates(xset XSetObject) ([]SubResourceTemplate, error)

    // BuildResource creates a subresource instance from template for a specific target
    BuildResource(ctx context.Context, xset XSetObject, template SubResourceTemplate, target client.Object, targetID string) (client.Object, error)

    // RetainWhenXSetDeleted returns true if subresource should be retained when XSet is deleted
    RetainWhenXSetDeleted(xset XSetObject) bool

    // RetainWhenXSetScaled returns true if subresource should be retained when XSet is scaled in
    RetainWhenXSetScaled(xset XSetObject) bool

    // AttachToTarget attaches subresources to target (e.g., mount PVC volumes to Pod)
    AttachToTarget(ctx context.Context, target client.Object, resources []client.Object) error

    // GetAttachedResourceNames returns names of subresources attached to target
    GetAttachedResourceNames(target client.Object) ([]string, error)
}

// SubResourceTemplate represents a parsed template with name and hash
type SubResourceTemplate struct {
    Name     string         // Template name (e.g., "data", "logs")
    Hash     string         // Hash of template spec for change detection
    Template client.Object  // Parsed template object
}
```

#### SubResourceAdapterGetter Interface

```go
// api/xset_controller_types.go - Add new optional interface

// SubResourceAdapterGetter is used to get subresource adapters.
// Implement this to enable generic subresource management.
type SubResourceAdapterGetter interface {
    GetSubResourceAdapters() []SubResourceAdapter
}
```

### 2. Name Truncation

#### NameTruncator

Uses Kubernetes standard constants from `k8s.io/apimachinery/pkg/util/validation`:

```go
// subresources/types.go

import (
    "fmt"
    "hash/fnv"
    "k8s.io/apimachinery/pkg/util/rand"
    "k8s.io/apimachinery/pkg/util/validation"
)

// NameTruncator handles resource name truncation within Kubernetes limits
type NameTruncator struct {
    MaxNameLength int
}

func NewNameTruncator() *NameTruncator {
    return &NameTruncator{
        MaxNameLength: validation.DNS1035LabelMaxLength, // 63
    }
}

// Truncate truncates name if exceeds max, appending hash suffix for uniqueness
func (t *NameTruncator) Truncate(name string) string {
    return t.TruncateWithMax(name, t.MaxNameLength)
}

// TruncateWithMax truncates name to specific max length
func (t *NameTruncator) TruncateWithMax(name string, maxLen int) string {
    if len(name) <= maxLen {
        return name
    }

    hash := computeHash(name)
    hashSuffix := fmt.Sprintf("-%s", hash)
    truncatedLen := maxLen - len(hashSuffix)

    if truncatedLen <= 0 {
        return hashSuffix[1:] // remove leading dash
    }

    return name[:truncatedLen] + hashSuffix
}

// TruncateLabelValue truncates label value to 63 chars
func (t *NameTruncator) TruncateLabelValue(value string) string {
    return t.TruncateWithMax(value, validation.LabelValueMaxLength)
}

func computeHash(s string) string {
    h := fnv.New32a()
    h.Write([]byte(s))
    return rand.SafeEncodeString(fmt.Sprint(h.Sum32()))[:6]
}
```

### 3. Label Value Handling

#### LabelManager

```go
// subresources/types.go

// LabelManager handles setting labels with automatic value truncation
type LabelManager struct {
    truncator *NameTruncator
}

func NewLabelManager(truncator *NameTruncator) *LabelManager {
    return &LabelManager{
        truncator: truncator,
    }
}

// SetLabel sets a label, truncating value if needed with hash suffix
func (lm *LabelManager) SetLabel(obj client.Object, key, value string) {
    if obj.GetLabels() == nil {
        obj.SetLabels(make(map[string]string))
    }

    truncatedValue := lm.truncator.TruncateLabelValue(value)
    obj.GetLabels()[key] = truncatedValue
}

// SetLabelWithTrackedOriginal sets label and tracks original value in annotation
func (lm *LabelManager) SetLabelWithTrackedOriginal(obj client.Object, key, value string) {
    truncatedValue := lm.truncator.TruncateLabelValue(value)

    if obj.GetLabels() == nil {
        obj.SetLabels(make(map[string]string))
    }
    obj.GetLabels()[key] = truncatedValue

    // Track original value in annotation if truncated
    if truncatedValue != value {
        if obj.GetAnnotations() == nil {
            obj.SetAnnotations(make(map[string]string))
        }
        obj.GetAnnotations()[key+".original"] = value
    }
}

// SetOperatingLabel sets operating label with ID in key
// Format: <prefix>/<id> = timestamp
func (lm *LabelManager) SetOperatingLabel(obj client.Object, prefix, id, value string) {
    truncatedID := lm.truncator.TruncateLabelValue(id)
    labelKey := fmt.Sprintf("%s/%s", prefix, truncatedID)

    if obj.GetLabels() == nil {
        obj.SetLabels(make(map[string]string))
    }
    obj.GetLabels()[labelKey] = value
}

// SetRevisionLabel sets revision info as label value
func (lm *LabelManager) SetRevisionLabel(obj client.Object, prefix, id, revisionName string) {
    truncatedID := lm.truncator.TruncateLabelValue(id)
    truncatedRevision := lm.truncator.TruncateLabelValue(revisionName)

    labelKey := fmt.Sprintf("%s/%s", prefix, truncatedID)

    if obj.GetLabels() == nil {
        obj.SetLabels(make(map[string]string))
    }
    obj.GetLabels()[labelKey] = truncatedRevision

    if truncatedRevision != revisionName {
        if obj.GetAnnotations() == nil {
            obj.SetAnnotations(make(map[string]string))
        }
        obj.GetAnnotations()[labelKey+".original-revision"] = revisionName
    }
}
```

### 4. Generic SubResourceControl

```go
// subresources/subresource_control.go

// SubResourceControl manages lifecycle of all subresource types
type SubResourceControl interface {
    // GetFilteredResources lists subresources owned by XSet
    GetFilteredResources(ctx context.Context, xset api.XSetObject, gvk schema.GroupVersionKind) ([]client.Object, error)

    // CreateTargetResources creates subresources for a target
    CreateTargetResources(ctx context.Context, xset api.XSetObject, target client.Object) error

    // DeleteTargetResources deletes subresources for a target
    DeleteTargetResources(ctx context.Context, xset api.XSetObject, target client.Object) error

    // DeleteUnusedResources removes subresources no longer in templates
    DeleteUnusedResources(ctx context.Context, xset api.XSetObject, target client.Object) error

    // AdoptOrphanedResources adopts resources left by retention policy
    AdoptOrphanedResources(ctx context.Context, xset api.XSetObject) ([]client.Object, error)

    // OrphanResources removes owner reference (for retention)
    OrphanResources(ctx context.Context, xset api.XSetObject, resources []client.Object) error

    // IsTemplateChanged checks if templates have changed
    IsTemplateChanged(ctx context.Context, xset api.XSetObject, target client.Object) (bool, error)
}

// RealSubResourceControl implements SubResourceControl using registered adapters
type RealSubResourceControl struct {
    client         client.Client
    scheme         *runtime.Scheme
    adapters       map[schema.GroupVersionKind]api.SubResourceAdapter
    expectations   *expectations.CacheExpectations
    labelAnnoMgr   api.XSetLabelAnnotationManager
    xsetController api.XSetController
    truncator      *NameTruncator
    labelManager   *LabelManager
}

// SubResourceControlBuilder builds control with registered adapters
type SubResourceControlBuilder struct {
    adapters []api.SubResourceAdapter
}

func NewSubResourceControlBuilder() *SubResourceControlBuilder {
    return &SubResourceControlBuilder{}
}

func (b *SubResourceControlBuilder) Register(adapter api.SubResourceAdapter) *SubResourceControlBuilder {
    b.adapters = append(b.adapters, adapter)
    return b
}

func (b *SubResourceControlBuilder) Build(
    mixin *mixin.ReconcilerMixin,
    xsetController api.XSetController,
    expectations *expectations.CacheExpectations,
    labelAnnoMgr api.XSetLabelAnnotationManager,
) (SubResourceControl, error) {
    adapters := make(map[schema.GroupVersionKind]api.SubResourceAdapter)
    for _, adapter := range b.adapters {
        adapters[adapter.Meta()] = adapter
    }

    truncator := NewNameTruncator()

    return &RealSubResourceControl{
        client:         mixin.Client,
        scheme:         mixin.Scheme,
        adapters:       adapters,
        expectations:   expectations,
        labelAnnoMgr:   labelAnnoMgr,
        xsetController: xsetController,
        truncator:      truncator,
        labelManager:   NewLabelManager(truncator),
    }, nil
}
```

### 5. PVC Adapter Implementation

```go
// subresources/pvc_adapter.go

// PvcSubResourceAdapter implements SubResourceAdapter for PVC
// Also implements old SubResourcePvcAdapter for backward compatibility
type PvcSubResourceAdapter struct {
    xsetController api.XSetController
    labelAnnoMgr   api.XSetLabelAnnotationManager
    truncator      *NameTruncator
    labelManager   *LabelManager
}

func NewPvcSubResourceAdapter(xsetController api.XSetController, labelAnnoMgr api.XSetLabelAnnotationManager) *PvcSubResourceAdapter {
    truncator := NewNameTruncator()
    return &PvcSubResourceAdapter{
        xsetController: xsetController,
        labelAnnoMgr:   labelAnnoMgr,
        truncator:      truncator,
        labelManager:   NewLabelManager(truncator),
    }
}

func (p *PvcSubResourceAdapter) Meta() schema.GroupVersionKind {
    return corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim")
}

func (p *PvcSubResourceAdapter) GetTemplates(xset api.XSetObject) ([]api.SubResourceTemplate, error) {
    // Use old interface for backward compatibility if XSetController implements it
    if pvcAdapter, ok := p.xsetController.(api.SubResourcePvcAdapter); ok {
        templates := pvcAdapter.GetXSetPvcTemplate(xset)
        var result []api.SubResourceTemplate
        for i := range templates {
            hash, err := PvcTemplateHash(&templates[i])
            if err != nil {
                return nil, err
            }
            result = append(result, api.SubResourceTemplate{
                Name:     templates[i].Name,
                Hash:     hash,
                Template: &templates[i],
            })
        }
        return result, nil
    }
    return nil, nil
}

func (p *PvcSubResourceAdapter) BuildResource(
    ctx context.Context,
    xset api.XSetObject,
    template api.SubResourceTemplate,
    target client.Object,
    targetID string,
) (client.Object, error) {
    pvc := template.Template.(*corev1.PersistentVolumeClaim).DeepCopy()

    // Generate name: xsetname-templatename-targetid
    baseName := fmt.Sprintf("%s-%s-%s", xset.GetName(), template.Name, targetID)
    pvc.Name = p.truncator.Truncate(baseName)
    pvc.Namespace = xset.GetNamespace()

    // Set owner reference
    xsetMeta := xset.GetObjectKind().GroupVersionKind()
    pvc.OwnerReferences = []metav1.OwnerReference{
        *metav1.NewControllerRef(xset, xsetMeta),
    }

    // Set labels
    if pvc.Labels == nil {
        pvc.Labels = make(map[string]string)
    }
    p.labelManager.SetLabel(pvc, p.labelAnnoMgr.Value(api.ControlledByXSetLabel), "true")
    p.labelManager.SetLabel(pvc, p.labelAnnoMgr.Value(api.XInstanceIdLabelKey), targetID)
    p.labelManager.SetLabelWithTrackedOriginal(pvc, p.labelAnnoMgr.Value(api.SubResourcePvcTemplateLabelKey), template.Name)
    p.labelManager.SetLabel(pvc, p.labelAnnoMgr.Value(api.SubResourcePvcTemplateHashLabelKey), template.Hash)

    return pvc, nil
}

func (p *PvcSubResourceAdapter) AttachToTarget(ctx context.Context, target client.Object, resources []client.Object) error {
    // Build volumes from PVCs and set on target
    var volumes []corev1.Volume
    for _, res := range resources {
        pvc := res.(*corev1.PersistentVolumeClaim)
        templateName := pvc.Labels[p.labelAnnoMgr.Value(api.SubResourcePvcTemplateLabelKey)]
        volumes = append(volumes, corev1.Volume{
            Name: templateName,
            VolumeSource: corev1.VolumeSource{
                PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
                    ClaimName: pvc.Name,
                },
            },
        })
    }

    // Use old interface to set volumes on target
    if pvcAdapter, ok := p.xsetController.(api.SubResourcePvcAdapter); ok {
        pvcAdapter.SetXSpecVolumes(target, volumes)
    }
    return nil
}

func (p *PvcSubResourceAdapter) RetainWhenXSetDeleted(xset api.XSetObject) bool {
    if pvcAdapter, ok := p.xsetController.(api.SubResourcePvcAdapter); ok {
        return pvcAdapter.RetainPvcWhenXSetDeleted(xset)
    }
    return false
}

func (p *PvcSubResourceAdapter) RetainWhenXSetScaled(xset api.XSetObject) bool {
    if pvcAdapter, ok := p.xsetController.(api.SubResourcePvcAdapter); ok {
        return pvcAdapter.RetainPvcWhenXSetScaled(xset)
    }
    return false
}

func (p *PvcSubResourceAdapter) GetAttachedResourceNames(target client.Object) ([]string, error) {
    if pvcAdapter, ok := p.xsetController.(api.SubResourcePvcAdapter); ok {
        volumes := pvcAdapter.GetXSpecVolumes(target)
        var names []string
        for _, v := range volumes {
            if v.PersistentVolumeClaim != nil {
                names = append(names, v.PersistentVolumeClaim.ClaimName)
            }
        }
        return names, nil
    }
    return nil, nil
}
```

### 6. Service Adapter Example

```go
// subresources/service_adapter.go

// ServiceSubResourceAdapter implements SubResourceAdapter for Service
type ServiceSubResourceAdapter struct {
    truncator    *NameTruncator
    labelManager *LabelManager
}

func NewServiceSubResourceAdapter() *ServiceSubResourceAdapter {
    truncator := NewNameTruncator()
    return &ServiceSubResourceAdapter{
        truncator:    truncator,
        labelManager: NewLabelManager(truncator),
    }
}

func (s *ServiceSubResourceAdapter) Meta() schema.GroupVersionKind {
    return corev1.SchemeGroupVersion.WithKind("Service")
}

func (s *ServiceSubResourceAdapter) GetTemplates(xset api.XSetObject) ([]api.SubResourceTemplate, error) {
    // Get from XSet spec or annotation
    // Example: annotation with JSON service templates
    // Or new field in XSetSpec
    return nil, nil // implement based on XSet type
}

func (s *ServiceSubResourceAdapter) BuildResource(
    ctx context.Context,
    xset api.XSetObject,
    template api.SubResourceTemplate,
    target client.Object,
    targetID string,
) (client.Object, error) {
    svc := template.Template.(*corev1.Service).DeepCopy()

    baseName := fmt.Sprintf("%s-%s-%s", xset.GetName(), template.Name, targetID)
    svc.Name = s.truncator.Truncate(baseName)
    svc.Namespace = xset.GetNamespace()

    // Set owner reference
    xsetMeta := xset.GetObjectKind().GroupVersionKind()
    svc.OwnerReferences = []metav1.OwnerReference{
        *metav1.NewControllerRef(xset, xsetMeta),
    }

    // Set labels for selection
    if svc.Labels == nil {
        svc.Labels = make(map[string]string)
    }
    s.labelManager.SetLabel(svc, "app.kubernetes.io/instance", targetID)

    return svc, nil
}

// Service doesn't need to "attach" to target in the same way PVC does
func (s *ServiceSubResourceAdapter) AttachToTarget(ctx context.Context, target client.Object, resources []client.Object) error {
    return nil // Services are independent, no attachment needed
}

func (s *ServiceSubResourceAdapter) GetAttachedResourceNames(target client.Object) ([]string, error) {
    return nil, nil // Services are independent, no attachment to target
}

func (s *ServiceSubResourceAdapter) RetainWhenXSetDeleted(xset api.XSetObject) bool {
    // Services are typically deleted with XSet by default
    return false
}

func (s *ServiceSubResourceAdapter) RetainWhenXSetScaled(xset api.XSetObject) bool {
    return false
}
```

## File Structure

```
kube-xset/
├── api/
│   ├── xset_controller_types.go    # Add SubResourceAdapterGetter interface
│   ├── subresource_types.go         # NEW: SubResourceAdapter, SubResourceTemplate interfaces
│   └── ... (other existing files)
├── subresources/
│   ├── subresource_control.go       # NEW: Generic SubResourceControl
│   ├── subresource_control_test.go  # NEW
│   ├── types.go                     # NEW: NameTruncator, LabelManager
│   ├── utils.go                     # NEW: Shared utilities (hash, etc.)
│   ├── getter.go                    # UPDATED: Add GetSubResourceAdapter()
│   ├── pvc_control.go               # KEPT for backward compatibility
│   ├── pvc_adapter.go               # NEW: PvcSubResourceAdapter
│   └── service_adapter.go           # NEW: ServiceSubResourceAdapter (example)
└── ... (other existing files)
```

## Migration Plan

### Phase 1: Add New Interfaces and Utilities (No Breaking Changes)

1. Add `api/subresource_types.go` with new interfaces
2. Add `subresources/types.go` with `NameTruncator`, `LabelManager`
3. Add `subresources/utils.go` with shared utilities
4. Update `subresources/getter.go` to add `GetSubResourceAdapter()` function

### Phase 2: Implement PVC Adapter with New Interface

1. Add `subresources/pvc_adapter.go` implementing both interfaces
2. Add `subresources/subresource_control.go` generic control
3. Update existing `pvc_control.go` to use new utilities internally

### Phase 3: Integration

1. Update `xset_controller.go` to use new `SubResourceControl`
2. Keep old `SubResourcePvcAdapter` path for backward compatibility
3. Prefer new `SubResourceAdapterGetter` if implemented

### Phase 4: Example Implementation

1. Add `subresources/service_adapter.go` as reference implementation
2. Update CLAUDE.md with new subresource documentation

## Backward Compatibility

| XSetController Implements | Behavior |
|--------------------------|----------|
| Neither interface | No subresource management |
| `SubResourcePvcAdapter` only | Uses old `PvcControl` (unchanged) |
| `SubResourceAdapterGetter` only | Uses new `SubResourceControl` |
| Both | Prefers new `SubResourceAdapterGetter` |

## Testing Strategy

1. **Unit Tests**
   - `NameTruncator.Truncate()` with various name lengths
   - `LabelManager.SetLabel()` with long values
   - Hash uniqueness for truncated names

2. **Integration Tests**
   - PVC adapter with new interface
   - Service adapter example
   - Backward compatibility with old `SubResourcePvcAdapter`

3. **E2E Tests**
   - Create XSet with long names, verify truncation
   - Update templates, verify subresource recreation
   - Delete XSet, verify retention policy

## Open Questions

1. Should we add a `ValidateTemplates()` method to `SubResourceAdapter` for admission validation?
2. Should `NameTruncator` be configurable per resource type (e.g., 253 for ConfigMaps)?
3. Should we track original names in annotations for debugging purposes?

## References

- Kubernetes naming constraints: `k8s.io/apimachinery/pkg/util/validation`
- Existing PVC implementation: `subresources/pvc_control.go`
- Example implementations:
  - ModelSet: `/Users/ana/projects/aicloud/modelops-controller/pkg/controllers/modelset/`
  - LeaderSet: `/Users/ana/projects/aicloud/aether/pkg/controller/leaderworkerset/leaderset/`