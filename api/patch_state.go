package api

// TargetPatchStateAnnotation is an observational snapshot, never planner input.
const TargetPatchStateAnnotation = "xset.kusionstack.io/patch-state"

// TargetPatchState records target-spec membership separately from readiness.
// Consumers must check the version, owner UID/generation and current revision.
type TargetPatchState struct {
	Version               int               `json:"version"`
	ObservedSetUID        string            `json:"observedSetUID"`
	ObservedSetGeneration int64             `json:"observedSetGeneration"`
	CurrentRevision       string            `json:"currentRevision"`
	DesiredRevision       string            `json:"desiredRevision"`
	AppliedPatchIDs       []string          `json:"appliedPatchIDs"`
	DesiredPatchIDs       []string          `json:"desiredPatchIDs"`
	CommandSnapshot       map[string]string `json:"commandSnapshot,omitempty"`
}
