# Target patch state observation

ConcurrentPatches writes `xset.kusionstack.io/patch-state` to each real target.
The version-1 JSON contract is `api.TargetPatchState`. It reports current and
desired revision names, applied/desired patch IDs, Set UID/generation and the
pending command snapshot. Membership refers to the target spec, not readiness.

Snapshots are never planner input. The revision catalog is still authoritative.
Consumers must reject a snapshot whose owner UID, generation or current revision
does not match the observed resources. New ByLabel commands invalidate the desired
observation until the controller republishes it. Synthetic revisions need no new
persistent objects: their membership is emitted while the catalog is available.

Each replacement peer uses its own current revision. Placeholders, excluded targets
and reclaimed scale-in identities are skipped. Legacy workloads do not emit snapshots.
Writes preserve other annotations, use UID/resourceVersion preconditions, and are
skipped when unchanged. The publishing path uses object copies so informer objects
are not mutated. Readiness timestamps may be nil; sorting treats nil as an unknown
time and preserves strict ordering.

Anchor consumes this wire contract independently, without importing XSet or copying
its planner. The ModelOps worktree vendors these source changes for local verification;
publish and pin a new XSet revision before refreshing that dependency from upstream.
