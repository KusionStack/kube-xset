package api

// RevisionPlanCompactionAnnotation distinguishes controller journal compaction
// from user patch deletion. Its value is the resourceVersion being compacted.
// Admission must verify the exact completed plan and unchanged template against
// oldObject; the marker alone never authorizes deleting active rollout state.
const RevisionPlanCompactionAnnotation = "xset.kusionstack.io/compact-revision-plan"
