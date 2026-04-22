# kube-xset Project Guide

kube-xset is a Kubernetes utility package for managing operations (scaling, upgrading, replacing) on a set of Kubernetes resources. It provides a reusable framework for building controllers that manage workload sets with advanced lifecycle management.

## Project Structure

```
kube-xset/
├── api/                    # Core API definitions and interfaces
│   ├── xset_controller_types.go  # XSetController interface (main entry point)
│   ├── xset_types.go              # XSetSpec, XSetStatus, update/scale strategies
│   ├── resourcecontext_types.go   # ResourceContext for ID allocation
│   ├── well_knowns.go             # Label/annotation constants
│   └── validation/                # Validation helpers
├── synccontrols/           # Core sync logic (Scale, Update, Replace)
│   ├── sync_control.go            # Main SyncControl interface
│   ├── x_scale.go                 # Scaling logic
│   ├── x_update.go                # Update logic
│   ├── x_replace.go               # Replace logic
│   └── inexclude.go               # Include/exclude targets
├── resourcecontexts/       # ResourceContext management (ID allocation)
├── opslifecycle/           # Ops lifecycle management (graceful operations)
├── xcontrol/               # Target control helpers
├── subresources/           # Subresource management (PVC)
├── revisionowner/          # Revision ownership tracking
├── features/               # Feature gates
└── xset_controller.go      # Main SetUpWithManager function
```

## How to Use kube-xset

### 1. Implement XSetController Interface

The main entry point is implementing the `XSetController` interface from `api/xset_controller_types.go`:

```go
type XSetController interface {
    ControllerName() string
    FinalizerName() string

    XSetMeta() metav1.TypeMeta      // GVK for XSet (e.g., CollaSet)
    XMeta() metav1.TypeMeta         // GVK for X (e.g., Pod)
    NewXSetObject() XSetObject      // Constructor for XSet
    NewXObject() client.Object      // Constructor for X
    NewXObjectList() client.ObjectList

    // Required interfaces
    XSetOperation                   // Access XSet spec/status
    XOperation                      // Access X object and status

    // Optional interfaces (implement as needed)
    // - LifecycleAdapterGetter
    // - ResourceContextAdapterGetter
    // - LabelAnnotationManagerGetter
    // - SubResourcePvcAdapter
    // - SubResourceAdapterGetter
    // - DecorationAdapter
}
```

### 2. Implement Required Interfaces

#### XSetOperation Interface

```go
type XSetOperation interface {
    GetXSetSpec(object XSetObject) *XSetSpec
    GetXSetPatch(object metav1.Object) ([]byte, error)
    GetXSetStatus(object XSetObject) *XSetStatus
    SetXSetStatus(object XSetObject, status *XSetStatus)
    UpdateScaleStrategy(ctx context.Context, c client.Client, object XSetObject, scaleStrategy *ScaleStrategy) error
    GetXSetTemplatePatcher(object metav1.Object) func(client.Object) error
}
```

**Example (CollaSet):**
```go
func (s *XSetOperation) GetXSetSpec(object xsetapi.XSetObject) *xsetapi.XSetSpec {
    set := object.(*CollaSet)
    return &xsetapi.XSetSpec{
        Replicas:       set.Spec.Replicas,
        Paused:         set.Spec.Paused,
        Selector:       set.Spec.Selector,
        UpdateStrategy: convertUpdateStrategy(set.Spec.UpdateStrategy),
        ScaleStrategy:  convertScaleStrategy(set.Spec.ScaleStrategy),
        HistoryLimit:   set.Spec.HistoryLimit,
    }
}
```

#### XOperation Interface

```go
type XOperation interface {
    GetXObjectFromRevision(revision *appsv1.ControllerRevision) (client.Object, error)
    CheckScheduled(object client.Object) bool
    CheckReadyTime(object client.Object) (bool, *metav1.Time)
    CheckAvailable(object client.Object) bool
    CheckInactive(object client.Object) bool
    GetXOpsPriority(ctx context.Context, c client.Client, object client.Object) (*OpsPriority, error)
}
```

### 3. Implement Optional Interfaces

#### ResourceContextAdapterGetter (for ID allocation)

```go
func (r *ResourceContextAdapterGetter) GetResourceContextAdapter() xsetapi.ResourceContextAdapter {
    return &MyResourceContextAdapter{}
}
```

#### LabelAnnotationManagerGetter (for custom labels)

```go
func (g *LabelManagerAdapterGetter) GetLabelManagerAdapter() map[xsetapi.XSetLabelAnnotationEnum]string {
    return map[xsetapi.XSetLabelAnnotationEnum]string{
        xsetapi.OperatingLabelPrefix:     "my-operator/operating",
        xsetapi.XInstanceIdLabelKey:      "my-operator/instance-id",
        // ... other labels
    }
}
```

#### SubResourcePvcAdapter (for PVC management)

```go
type SubResourcePvcAdapter interface {
    RetainPvcWhenXSetDeleted(object XSetObject) bool
    RetainPvcWhenXSetScaled(object XSetObject) bool
    GetXSetPvcTemplate(object XSetObject) []corev1.PersistentVolumeClaim
    GetXSpecVolumes(object client.Object) []corev1.Volume
    GetXVolumeMounts(object client.Object) []corev1.VolumeMount
    SetXSpecVolumes(object client.Object, volumes []corev1.Volume)
}
```

#### SubResourceAdapterGetter (for generic subresource management)

Controllers implementing `SubResourcePvcAdapter` are automatically bridged to `SubResourceAdapter` via `BuildAdapters()`. For custom subresource types, implement `SubResourceAdapterGetter`:

```go
func (c *MyXSetController) GetSubResourceAdapters() []xsetapi.SubResourceAdapter {
    return []xsetapi.SubResourceAdapter{
        // Add custom adapters as needed
    }
}
```

#### SubResourceAdapter Interface

The `SubResourceAdapter` interface provides a generic way to manage subresources:

```go
type SubResourceAdapter interface {
    Meta() schema.GroupVersionKind
    GetTemplates(xset XSetObject) ([]SubResourceTemplate, error)
    RetainWhenXSetDeleted(xset XSetObject) bool
    RetainWhenXSetScaled(xset XSetObject) bool
    RecreateWhenXSetUpdated(xset XSetObject) bool
    AttachToTarget(ctx context.Context, target client.Object, resources []client.Object) error
}

// Optional interface for customizing resources
type SubResourceDecorator interface {
    DecorateResource(ctx context.Context, xset XSetObject, template SubResourceTemplate, resource client.Object, target client.Object, targetID string) error
}
```

The control code creates resources from templates and sets namespace, owner reference, and labels. Implement `SubResourceDecorator` to customize the resource (e.g., set Name, add custom labels).

#### Name Truncation

Resource names are automatically truncated to 63 characters with a hash suffix for uniqueness:

```go
truncator := subresources.NewNameTruncator()
name := truncator.Truncate("very-long-resource-name-exceeding-63-characters-limit")
// Result: "very-long-resource-name-exceeding-63-charact-abc123"
```

#### Label Value Handling

Label values are automatically truncated with original value tracking:

```go
lm := subresources.NewLabelManager(truncator)
lm.SetLabel(obj, "key", "very-long-label-value")
lm.SetLabelWithTrackedOriginal(obj, "key", "original-value-tracked-in-annotation")
```

#### DecorationAdapter (for decoration/patcher management)

```go
type DecorationAdapter interface {
    WatchDecoration(c controller.Controller) error
    GetDecorationGroupVersionKind() metav1.GroupVersionKind
    GetTargetCurrentDecorationRevisions(ctx context.Context, c client.Client, target client.Object) (string, error)
    GetTargetUpdatedDecorationRevisions(ctx context.Context, c client.Client, target client.Object) (string, error)
    GetDecorationPatcherByRevisions(ctx context.Context, c client.Client, target client.Object, revision string) (func(client.Object) error, error)
    IsTargetDecorationChanged(currentRevision, updatedRevision string) (bool, error)
}
```

### 4. Register Controller

Use `SetUpWithManager` to register your controller:

```go
func Add(mgr ctrl.Manager) error {
    xsetController := &MyXSetController{}
    return xset.SetupWithManager(mgr, xsetController)
}
```

## Supported Features

### Update Strategies

| Strategy | Description |
|----------|-------------|
| `Recreate` | Delete and recreate targets on update |
| `InPlaceIfPossible` | In-place update if possible, fall back to recreate |
| `InPlaceOnly` | Always in-place update (requires special K8s cluster) |
| `Replace` | Create new target, wait for ready, then delete old |

### Rolling Update Control

- **ByPartition**: Control update progress by partition value
- **ByLabel**: Control update by attaching target labels

### Scale Strategies

- **Context Pool**: Share instance IDs between multiple XSets
- **TargetToInclude/Exclude**: Include/exclude specific targets
- **TargetToDelete**: Delete specific targets

### OpsLifecycle

Provides graceful operation lifecycle:
1. **Begin**: Start operation lifecycle
2. **AllowOps**: Check if operation is allowed (with delay)
3. **Finish**: Complete operation lifecycle

Key functions in `opslifecycle/utils.go`:
- `Begin()` - Begin lifecycle
- `AllowOps()` - Check permission with delay
- `Finish()` - Finish lifecycle
- `IsDuringOps()` - Check if in lifecycle

### ResourceContext

Manages instance ID allocation across targets:
- `AllocateID()` - Allocate IDs for targets
- `CleanUnusedIDs()` - Clean unused IDs
- `UpdateToTargetContext()` - Update context

## Label/Annotation Keys

Key labels defined in `api/well_knowns.go`:

| Label Type | Purpose |
|------------|---------|
| `OperatingLabelPrefix` | Target under operation |
| `OperationTypeLabelPrefix` | Type of operation |
| `OperateLabelPrefix` | Target can start operation |
| `XInstanceIdLabelKey` | Instance ID for target |
| `PreparingDeleteLabel` | Target preparing delete |
| `ControlledByXSetLabel` | Target controlled by XSet |

## Example Implementations

### CollaSet (kuperator)

GitHub: https://github.com/KusionStack/kuperator

Location: `pkg/controllers/collaset/`

Key files:
- `collaset_controller.go` - XSetController implementation
- `collaset_adapter.go` - Adapters for XSetOperation, XOperation
- `resource_context.go` - ResourceContext adapter
- `lifecycle_adapter.go` - Lifecycle adapters

CollaSet is the original implementation that kube-xset was extracted from. It manages Pod workloads with PodDecoration support for in-place updates.

## Key Workflow

The reconcile loop (in `xset_controller.go`):

1. **SyncTargets**: Parse targets, allocate IDs, manage include/exclude
2. **Replace**: Handle replace-indicated targets
3. **Scale**: Scale out/in targets with lifecycle
4. **Update**: Update targets to new revision
5. **CalculateStatus**: Compute and update status

## Testing

Tests are located alongside source files (e.g., `*_test.go`). Key test patterns:

- Use `suite` pattern for integration tests
- Mock clients for unit tests
- Focus on sync control logic

## Import

```go
import "kusionstack.io/kube-xset"
import xsetapi "kusionstack.io/kube-xset/api"
```

## Dependencies

- `kusionstack.io/kube-utils` - Controller utilities
- `kusionstack.io/kube-api` - KusionStack API definitions
- Standard controller-runtime libraries