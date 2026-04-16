# Configurable Naming Prefix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add optional interface methods to allow controllers and adapters to customize target and subresource name prefixes.

**Architecture:** Add `GetTargetPrefix` to XSetController interface and `GetSubResourcePrefix` to SubResourceAdapter interface. Both methods are optional - if not implemented or return empty string, default naming is used. Truncation responsibility moves to the controller/adapter implementation.

**Tech Stack:** Go, Kubernetes controller-runtime, kube-xset framework

---

## Task 1: Add GetTargetPrefix to XSetController Interface

**Files:**
- Modify: `api/xset_controller_types.go`

- [ ] **Step 1: Add GetTargetPrefix method to XSetController interface**

Add the optional method to the XSetController interface in `api/xset_controller_types.go`:

```go
// XSetController is the interface that XSet controllers must implement.
type XSetController interface {
    // ... existing methods ...

    // Optional: GetTargetPrefix returns the prefix for target names.
    // If not implemented (returns empty string), defaults to "{xset-name}-".
    // Controller is responsible for truncation if needed.
    // The returned prefix should end with "-" if a separator is desired.
    GetTargetPrefix(xset XSetObject) string
}
```

- [ ] **Step 2: Commit API change**

```bash
git add api/xset_controller_types.go
git commit -m "feat(api): add GetTargetPrefix to XSetController interface

Add optional method for customizing target name prefixes.
Controllers can implement this to override the default naming pattern.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 2: Add GetSubResourcePrefix to SubResourceAdapter Interface

**Files:**
- Modify: `api/subresource_types.go`

- [ ] **Step 1: Add GetSubResourcePrefix method to SubResourceAdapter interface**

Add the optional method to the SubResourceAdapter interface in `api/subresource_types.go`:

```go
// SubResourceAdapter is the interface for managing subresources.
type SubResourceAdapter interface {
    // ... existing methods ...

    // Optional: GetSubResourcePrefix returns the prefix for subresource names.
    // If not implemented (returns empty string), defaults to "{xset-name}-{template-name}-".
    // Adapter is responsible for truncation if needed.
    // The returned prefix should end with "-" if a separator is desired.
    GetSubResourcePrefix(xset XSetObject, template SubResourceTemplate) string
}
```

- [ ] **Step 2: Commit API change**

```bash
git add api/subresource_types.go
git commit -m "feat(api): add GetSubResourcePrefix to SubResourceAdapter interface

Add optional method for customizing subresource name prefixes.
Adapters can implement this to override the default naming pattern.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 3: Update GetTargetsPrefix Helper Function

**Files:**
- Modify: `synccontrols/x_utils.go`

- [ ] **Step 1: Update GetTargetsPrefix to accept override parameter**

Modify the `GetTargetsPrefix` function in `synccontrols/x_utils.go`:

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

- [ ] **Step 2: Commit helper function change**

```bash
git add synccontrols/x_utils.go
git commit -m "feat(synccontrols): update GetTargetsPrefix to accept override

Simplify GetTargetsPrefix to accept an optional override parameter.
Truncation logic moved to controller implementation.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 4: Update NewTargetFrom to Use Prefix Override

**Files:**
- Modify: `synccontrols/x_utils.go`

- [ ] **Step 1: Update NewTargetFrom to check for GetTargetPrefix implementation**

Modify `NewTargetFrom` in `synccontrols/x_utils.go` to check for the optional interface:

```go
func NewTargetFrom(setController api.XSetController, xsetLabelAnnoMgr api.XSetLabelAnnotationManager, owner api.XSetObject, revision *appsv1.ControllerRevision, id int, updateFuncs ...func(client.Object) error) (client.Object, error) {
    targetObj, err := setController.GetXObjectFromRevision(revision)
    if err != nil {
        return nil, err
    }

    meta := setController.XSetMeta()
    ownerRef := metav1.NewControllerRef(owner, meta.GroupVersionKind())
    targetObj.SetOwnerReferences(append(targetObj.GetOwnerReferences(), *ownerRef))
    targetObj.SetNamespace(owner.GetNamespace())

    // Get prefix from controller (may be empty for default)
    var prefixOverride string
    if pg, ok := setController.(interface{ GetTargetPrefix(api.XSetObject) string }); ok {
        prefixOverride = pg.GetTargetPrefix(owner)
    }
    targetObj.SetGenerateName(GetTargetsPrefix(prefixOverride, owner.GetName()))

    if IsTargetNamingSuffixPolicyPersistentSequence(setController.GetXSetSpec(owner)) {
        targetObj.SetName(fmt.Sprintf("%s%d", targetObj.GetGenerateName(), id))
    }

    xsetLabelAnnoMgr.Set(targetObj, api.XInstanceIdLabelKey, fmt.Sprintf("%d", id))
    targetObj.GetLabels()[appsv1.ControllerRevisionHashLabelKey] = revision.GetName()
    controlByXSet(xsetLabelAnnoMgr, targetObj)

    for _, fn := range updateFuncs {
        if err := fn(targetObj); err != nil {
            return targetObj, err
        }
    }

    return targetObj, nil
}
```

- [ ] **Step 2: Commit NewTargetFrom change**

```bash
git add synccontrols/x_utils.go
git commit -m "feat(synccontrols): use GetTargetPrefix in NewTargetFrom

Check for optional GetTargetPrefix implementation and use custom prefix
if provided. Falls back to default naming if not implemented.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 5: Add GetSubResourcePrefix Helper Function

**Files:**
- Modify: `subresources/utils.go`

- [ ] **Step 1: Add GetSubResourcePrefix helper function**

Add the helper function to `subresources/utils.go`:

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

- [ ] **Step 2: Commit helper function**

```bash
git add subresources/utils.go
git commit -m "feat(subresources): add GetSubResourcePrefix helper

Add helper function for subresource prefix generation with override support.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 6: Update SubResourceControl to Use Prefix Override

**Files:**
- Modify: `subresources/subresource_control.go`

- [ ] **Step 1: Find the resource building location**

Find where subresource names are generated (look for `GenerateName` or `SetName` in the file).

- [ ] **Step 2: Update buildResource to check for GetSubResourcePrefix**

Add interface check and use custom prefix. The exact location depends on the current implementation, but the pattern is:

```go
// Get prefix from adapter (may be empty for default)
var prefixOverride string
if pg, ok := adapter.(interface{ GetSubResourcePrefix(api.XSetObject, api.SubResourceTemplate) string }); ok {
    prefixOverride = pg.GetSubResourcePrefix(xset, template)
}
resource.SetGenerateName(GetSubResourcePrefix(prefixOverride, xset.GetName(), template.GetName()))
```

- [ ] **Step 3: Commit SubResourceControl change**

```bash
git add subresources/subresource_control.go
git commit -m "feat(subresources): use GetSubResourcePrefix in SubResourceControl

Check for optional GetSubResourcePrefix implementation and use custom prefix
if provided. Falls back to default naming if not implemented.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 7: Add Unit Tests for GetTargetsPrefix

**Files:**
- Modify: `synccontrols/x_utils_test.go` (create if doesn't exist)

- [ ] **Step 1: Write tests for GetTargetsPrefix with override**

```go
func TestGetTargetsPrefix(t *testing.T) {
    tests := []struct {
        name          string
        override      string
        controllerName string
        expected      string
    }{
        {
            name:          "default naming",
            override:      "",
            controllerName: "myset",
            expected:      "myset-",
        },
        {
            name:          "custom prefix",
            override:      "custom-",
            controllerName: "myset",
            expected:      "custom-",
        },
        {
            name:          "custom prefix without dash",
            override:      "custom",
            controllerName: "myset",
            expected:      "custom",
        },
        {
            name:          "empty override uses default",
            override:      "",
            controllerName: "test-controller",
            expected:      "test-controller-",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := GetTargetsPrefix(tt.override, tt.controllerName)
            assert.Equal(t, tt.expected, result)
        })
    }
}
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
cd /Users/ana/projects/aicloud/kube-xset
go test ./synccontrols/... -v -run TestGetTargetsPrefix
```

Expected: All tests pass

- [ ] **Step 3: Commit tests**

```bash
git add synccontrols/x_utils_test.go
git commit -m "test(synccontrols): add tests for GetTargetsPrefix with override

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 8: Add Unit Tests for GetSubResourcePrefix

**Files:**
- Modify: `subresources/utils_test.go` (create if doesn't exist)

- [ ] **Step 1: Write tests for GetSubResourcePrefix with override**

```go
func TestGetSubResourcePrefix(t *testing.T) {
    tests := []struct {
        name         string
        override     string
        xsetName     string
        templateName string
        expected     string
    }{
        {
            name:         "default naming",
            override:     "",
            xsetName:     "myset",
            templateName: "data",
            expected:     "myset-data-",
        },
        {
            name:         "custom prefix",
            override:     "custom-",
            xsetName:     "myset",
            templateName: "data",
            expected:     "custom-",
        },
        {
            name:         "custom prefix without dash",
            override:     "custom",
            xsetName:     "myset",
            templateName: "data",
            expected:     "custom",
        },
        {
            name:         "empty override uses default",
            override:     "",
            xsetName:     "test-set",
            templateName: "pvc-template",
            expected:     "test-set-pvc-template-",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := GetSubResourcePrefix(tt.override, tt.xsetName, tt.templateName)
            assert.Equal(t, tt.expected, result)
        })
    }
}
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
cd /Users/ana/projects/aicloud/kube-xset
go test ./subresources/... -v -run TestGetSubResourcePrefix
```

Expected: All tests pass

- [ ] **Step 3: Commit tests**

```bash
git add subresources/utils_test.go
git commit -m "test(subresources): add tests for GetSubResourcePrefix with override

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 9: Test with Aether LeaderSet Controller - Target Prefix

**Files:**
- Modify: `/Users/ana/projects/aicloud/aether/pkg/controller/leaderworkerset/leaderset/leaderset_adapter.go`
- Modify: `/Users/ana/projects/aicloud/aether/pkg/controller/leaderworkerset/leaderset/leaderset_adapter_test.go`

- [ ] **Step 1: Add GetTargetPrefix to LeaderSetXSetController**

Add the method to `leaderset_adapter.go`:

```go
// GetTargetPrefix returns a custom prefix for target (leader pod) names.
// Uses annotation if present, otherwise returns empty for default behavior.
func (c *LeaderSetXSetController) GetTargetPrefix(xset api.XSetObject) string {
    lws, ok := xset.(*corev1beta1.LeaderWorkerSet)
    if !ok {
        return ""
    }

    // Check for custom prefix annotation
    if prefix := lws.Annotations[LeaderSetTargetPrefixAnnotation]; prefix != "" {
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

Add the constant at the top of the file:

```go
const (
    // ... existing constants ...
    
    // LeaderSetTargetPrefixAnnotation is the annotation key for custom target name prefix.
    LeaderSetTargetPrefixAnnotation = "leaderworkerset.theta.alipay.com/target-prefix"
)
```

- [ ] **Step 2: Write test for GetTargetPrefix**

Add test to `leaderset_adapter_test.go`:

```go
func TestLeaderSetXSetController_GetTargetPrefix(t *testing.T) {
    controller := &LeaderSetXSetController{}

    t.Run("returns empty when no annotation", func(t *testing.T) {
        lws := &corev1beta1.LeaderWorkerSet{
            ObjectMeta: metav1.ObjectMeta{
                Name: "test-lws",
            },
        }
        result := controller.GetTargetPrefix(lws)
        assert.Empty(t, result, "Should return empty for default behavior")
    })

    t.Run("returns custom prefix from annotation", func(t *testing.T) {
        lws := &corev1beta1.LeaderWorkerSet{
            ObjectMeta: metav1.ObjectMeta{
                Name: "test-lws",
                Annotations: map[string]string{
                    LeaderSetTargetPrefixAnnotation: "custom-prefix",
                },
            },
        }
        result := controller.GetTargetPrefix(lws)
        assert.Equal(t, "custom-prefix-", result)
    })

    t.Run("truncates long prefix", func(t *testing.T) {
        longPrefix := strings.Repeat("a", 60)
        lws := &corev1beta1.LeaderWorkerSet{
            ObjectMeta: metav1.ObjectMeta{
                Name: "test-lws",
                Annotations: map[string]string{
                    LeaderSetTargetPrefixAnnotation: longPrefix,
                },
            },
        }
        result := controller.GetTargetPrefix(lws)
        assert.LessOrEqual(t, len(result), 53) // 52 + dash
    })

    t.Run("removes trailing dash and adds it back", func(t *testing.T) {
        lws := &corev1beta1.LeaderWorkerSet{
            ObjectMeta: metav1.ObjectMeta{
                Name: "test-lws",
                Annotations: map[string]string{
                    LeaderSetTargetPrefixAnnotation: "custom-",
                },
            },
        }
        result := controller.GetTargetPrefix(lws)
        assert.Equal(t, "custom-", result)
    })
}
```

- [ ] **Step 3: Run tests to verify they pass**

```bash
cd /Users/ana/projects/aicloud/aether
go test ./pkg/controller/leaderworkerset/leaderset/... -v -run TestLeaderSetXSetController_GetTargetPrefix
```

Expected: All tests pass

- [ ] **Step 4: Commit aether changes**

```bash
cd /Users/ana/projects/aicloud/aether
git add pkg/controller/leaderworkerset/leaderset/leaderset_adapter.go
git add pkg/controller/leaderworkerset/leaderset/leaderset_adapter_test.go
git commit -m "feat(leaderset): implement GetTargetPrefix for custom pod naming

Add optional GetTargetPrefix method to LeaderSetXSetController.
Supports custom target name prefix via annotation.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 10: Test with Aether LeaderSet Controller - SubResource Prefix

**Files:**
- Modify: `/Users/ana/projects/aicloud/aether/pkg/controller/leaderworkerset/leaderset/pvc_adapter.go`
- Modify: `/Users/ana/projects/aicloud/aether/pkg/controller/leaderworkerset/leaderset/pvc_adapter_test.go`

- [ ] **Step 1: Add GetSubResourcePrefix to LeaderSetPvcSubResourceAdapter**

Add the method to `pvc_adapter.go`:

```go
// GetSubResourcePrefix returns a custom prefix for PVC names.
// Uses annotation if present, otherwise returns empty for default behavior.
func (p *LeaderSetPvcSubResourceAdapter) GetSubResourcePrefix(xset api.XSetObject, template api.SubResourceTemplate) string {
    lws, ok := xset.(*corev1beta1.LeaderWorkerSet)
    if !ok {
        return ""
    }

    // Check for custom prefix annotation
    if prefix := lws.Annotations[LeaderSetPvcPrefixAnnotation]; prefix != "" {
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

Add the constant at the top of the file:

```go
const (
    // LeaderSetPvcPrefixAnnotation is the annotation key for custom PVC name prefix.
    LeaderSetPvcPrefixAnnotation = "leaderworkerset.theta.alipay.com/pvc-prefix"
)
```

- [ ] **Step 2: Write test for GetSubResourcePrefix**

Add test to `pvc_adapter_test.go`:

```go
func TestLeaderSetPvcSubResourceAdapter_GetSubResourcePrefix(t *testing.T) {
    adapter := NewLeaderSetPvcSubResourceAdapter()

    t.Run("returns empty when no annotation", func(t *testing.T) {
        lws := &corev1beta1.LeaderWorkerSet{
            ObjectMeta: metav1.ObjectMeta{
                Name: "test-lws",
            },
        }
        template := api.SubResourceTemplate{Name: "data"}
        result := adapter.GetSubResourcePrefix(lws, template)
        assert.Empty(t, result, "Should return empty for default behavior")
    })

    t.Run("returns custom prefix from annotation", func(t *testing.T) {
        lws := &corev1beta1.LeaderWorkerSet{
            ObjectMeta: metav1.ObjectMeta{
                Name: "test-lws",
                Annotations: map[string]string{
                    LeaderSetPvcPrefixAnnotation: "custom-pvc",
                },
            },
        }
        template := api.SubResourceTemplate{Name: "data"}
        result := adapter.GetSubResourcePrefix(lws, template)
        assert.Equal(t, "custom-pvc-", result)
    })

    t.Run("truncates long prefix", func(t *testing.T) {
        longPrefix := strings.Repeat("a", 60)
        lws := &corev1beta1.LeaderWorkerSet{
            ObjectMeta: metav1.ObjectMeta{
                Name: "test-lws",
                Annotations: map[string]string{
                    LeaderSetPvcPrefixAnnotation: longPrefix,
                },
            },
        }
        template := api.SubResourceTemplate{Name: "data"}
        result := adapter.GetSubResourcePrefix(lws, template)
        assert.LessOrEqual(t, len(result), 53) // 52 + dash
    })
}
```

- [ ] **Step 3: Run tests to verify they pass**

```bash
cd /Users/ana/projects/aicloud/aether
go test ./pkg/controller/leaderworkerset/leaderset/... -v -run TestLeaderSetPvcSubResourceAdapter_GetSubResourcePrefix
```

Expected: All tests pass

- [ ] **Step 4: Commit aether changes**

```bash
cd /Users/ana/projects/aicloud/aether
git add pkg/controller/leaderworkerset/leaderset/pvc_adapter.go
git add pkg/controller/leaderworkerset/leaderset/pvc_adapter_test.go
git commit -m "feat(leaderset): implement GetSubResourcePrefix for custom PVC naming

Add optional GetSubResourcePrefix method to LeaderSetPvcSubResourceAdapter.
Supports custom PVC name prefix via annotation.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 11: Run Full Test Suite

**Files:**
- None (verification task)

- [ ] **Step 1: Run kube-xset tests**

```bash
cd /Users/ana/projects/aicloud/kube-xset
go test ./... -v
```

Expected: All tests pass

- [ ] **Step 2: Run aether tests**

```bash
cd /Users/ana/projects/aicloud/aether
go test ./pkg/controller/leaderworkerset/leaderset/... -v
```

Expected: All tests pass

---

## Task 12: Final Commit and Push

**Files:**
- None (finalization task)

- [ ] **Step 1: Review all changes**

```bash
cd /Users/ana/projects/aicloud/kube-xset
git log --oneline main..HEAD
```

- [ ] **Step 2: Push kube-xset changes**

```bash
cd /Users/ana/projects/aicloud/kube-xset
git push origin feature/generic-subresource
```

- [ ] **Step 3: Push aether changes**

```bash
cd /Users/ana/projects/aicloud/aether
git push origin <current-branch>
```