# Configurable Naming Prefix Design

**Date:** 2026-04-16
**Status:** Draft
**Author:** kube-xset team

## Summary

Add optional interface methods to allow controllers and adapters to customize target and subresource name prefixes. This enables naming customization for readability and organizational conventions.

## Motivation

Currently, kube-xset uses hardcoded naming patterns:
- **Target names:** `{xset-name}-{suffix}` (e.g., `myset-abc123`)
- **Subresource names:** `{xset-name}-{template-name}-{suffix}` (e.g., `myset-data-abc123`)

Users want the ability to customize these prefixes for:
- Shorter or more descriptive names
- Organizational naming conventions
- Integration with external tooling that expects specific patterns

## Goals

- Allow controllers to customize target name prefixes
- Allow adapters to customize subresource name prefixes
- Maintain full backward compatibility
- Keep truncation logic in controller/adapter implementations

## Non-Goals

- Changing suffix generation logic (Random/PersistentSequence remain unchanged)
- Adding declarative configuration fields to specs
- Providing truncation utilities in kube-xset core

## Design

### API Changes

#### XSetController Interface

Add optional method to `api/xset_controller_types.go`:

```go
// XSetController is the interface that XSet controllers must implement.
type XSetController interface {
    // ... existing methods ...

    // Optional: GetTargetPrefix returns the prefix for target names.
    // If not implemented (returns empty string), defaults to "{xset-name}-".
    // Controller is responsible for truncation if needed.
    GetTargetPrefix(xset XSetObject) string
}
```

#### SubResourceAdapter Interface

Add optional method to `api/subresource_types.go`:

```go
// SubResourceAdapter is the interface for managing subresources.
type SubResourceAdapter interface {
    // ... existing methods ...

    // Optional: GetSubResourcePrefix returns the prefix for subresource names.
    // If not implemented (returns empty string), defaults to "{xset-name}-{template-name}-".
    // Adapter is responsible for truncation if needed.
    GetSubResourcePrefix(xset XSetObject, template SubResourceTemplate) string
}
```

### Implementation

#### synccontrols/x_utils.go

Simplify `GetTargetsPrefix` to accept an override:

```go
// GetTargetsPrefix returns the prefix for target names.
// If override is non-empty, uses it; otherwise uses controllerName.
func GetTargetsPrefix(override, controllerName string) string {
    if override != "" {
        return override
    }
    return fmt.Sprintf("%s-", controllerName)
}
```

Update `NewTargetFrom` to check for prefix override:

```go
func NewTargetFrom(setController api.XSetController, xsetLabelAnnoMgr api.XSetLabelAnnotationManager, owner api.XSetObject, revision *appsv1.ControllerRevision, id int, updateFuncs ...func(client.Object) error) (client.Object, error) {
    targetObj, err := setController.GetXObjectFromRevision(revision)
    if err != nil {
        return nil, err
    }

    // ... existing owner ref, namespace setup ...

    // Get prefix from controller (may be empty for default)
    var prefixOverride string
    if pg, ok := setController.(interface{ GetTargetPrefix(api.XSetObject) string }); ok {
        prefixOverride = pg.GetTargetPrefix(owner)
    }
    targetObj.SetGenerateName(GetTargetsPrefix(prefixOverride, owner.GetName()))

    // ... rest of existing logic (naming suffix policy, labels, etc.) ...
}
```

#### subresources package

Add helper function in `subresources/utils.go`:

```go
// GetSubResourcePrefix returns the prefix for subresource names.
// If override is non-empty, uses it; otherwise uses "{xsetName}-{templateName}-".
func GetSubResourcePrefix(override, xsetName, templateName string) string {
    if override != "" {
        return override
    }
    return fmt.Sprintf("%s-%s-", xsetName, templateName)
}
```

Update `SubResourceControl` to check for prefix override when building resources:

```go
func (sc *RealSubResourceControl) buildResource(ctx context.Context, xset api.XSetObject, template api.SubResourceTemplate, target client.Object, targetID string) (client.Object, error) {
    adapter := sc.getAdapter(template)
    resource, err := adapter.BuildResource(ctx, xset, template, target, targetID)
    if err != nil {
        return nil, err
    }

    // Get prefix from adapter (may be empty for default)
    var prefixOverride string
    if pg, ok := adapter.(interface{ GetSubResourcePrefix(api.XSetObject, api.SubResourceTemplate) string }); ok {
        prefixOverride = pg.GetSubResourcePrefix(xset, template)
    }
    resource.SetGenerateName(GetSubResourcePrefix(prefixOverride, xset.GetName(), template.GetName()))

    // ... rest of existing logic ...
}
```

### Backward Compatibility

| Scenario | Behavior |
|----------|----------|
| Controller doesn't implement `GetTargetPrefix` | Uses `{xset-name}-` as prefix |
| Adapter doesn't implement `GetSubResourcePrefix` | Uses `{xset-name}-{template-name}-` as prefix |
| Method returns empty string | Treated same as not implemented (uses default) |
| Method returns custom prefix | Uses returned value directly (no modification) |

### Truncation Responsibility

The truncation logic is removed from kube-xset core. Controllers and adapters are responsible for ensuring their custom prefixes comply with Kubernetes naming constraints:
- DNS labels: max 63 characters
- Leave room for suffix (typically 5-10 characters for random suffix)

Controllers can implement truncation in their `GetTargetPrefix` method:

```go
func (c *MyController) GetTargetPrefix(xset api.XSetObject) string {
    prefix := computeCustomPrefix(xset)
    // Truncate if needed, leaving room for suffix
    if len(prefix) > 52 {
        prefix = prefix[:52]
    }
    return prefix + "-"
}
```

## Example Usage

### Controller with Custom Target Prefix

```go
type ModelSetController struct {
    // ... fields ...
}

// GetTargetPrefix returns a custom prefix based on annotation.
func (c *ModelSetController) GetTargetPrefix(xset api.XSetObject) string {
    // Check for custom prefix annotation
    if prefix := xset.GetAnnotations()["modelops.kusionstack.io/target-prefix"]; prefix != "" {
        // Ensure it ends with dash and fits DNS label limits
        prefix = strings.TrimSuffix(prefix, "-")
        if len(prefix) > 52 {
            prefix = prefix[:52]
        }
        return prefix + "-"
    }
    // Return empty for default behavior
    return ""
}
```

### Adapter with Custom Subresource Prefix

```go
type PvcSubResourceAdapter struct {
    // ... fields ...
}

// GetSubResourcePrefix returns a custom prefix for PVC names.
func (a *PvcSubResourceAdapter) GetSubResourcePrefix(xset api.XSetObject, template api.SubResourceTemplate) string {
    // Use shorter prefix: just template name
    prefix := template.GetName()
    if len(prefix) > 52 {
        prefix = prefix[:52]
    }
    return prefix + "-"
}
```

## Testing

1. **Unit tests for helper functions:**
   - `GetTargetsPrefix` with override and default
   - `GetSubResourcePrefix` with override and default

2. **Unit tests for interface detection:**
   - Controller without method → uses default
   - Controller with method returning empty → uses default
   - Controller with method returning custom → uses custom

3. **Integration tests:**
   - End-to-end test with custom prefix controller
   - End-to-end test with custom prefix adapter
   - Backward compatibility test with existing controllers

## Migration Guide

No migration required. Existing controllers and adapters continue to work unchanged. To customize naming:

1. Implement `GetTargetPrefix(xset XSetObject) string` on your controller
2. Implement `GetSubResourcePrefix(xset XSetObject, template SubResourceTemplate) string` on your adapter
3. Handle truncation in your implementation if needed

## Alternatives Considered

1. **Configuration fields in spec** - Less flexible, doesn't support dynamic prefix generation
2. **Dedicated NamingAdapter interface** - More interfaces to manage, harder to discover
3. **Keep truncation in core** - Adds complexity and assumptions about suffix length