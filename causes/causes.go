package causes

import (
	"time"

	"k8s.io/apimachinery/pkg/types"
)

type CauseKind = string
type EventKind = string

const (
	CausePrimaryCreate       CauseKind = "PrimaryCreate"
	CausePrimarySpecUpdate   CauseKind = "PrimarySpecUpdate"
	CausePrimaryStatusOrMeta CauseKind = "PrimaryStatusOrMetadata"
	CausePrimaryDelete       CauseKind = "PrimaryDelete"
	CausePrimaryGeneric      CauseKind = "PrimaryGeneric"
	CausePrimaryUnknown      CauseKind = "PrimaryUnknownOrRequeue"

	EventCreate  EventKind = "Create"
	EventUpdate  EventKind = "Update"
	EventDelete  EventKind = "Delete"
	EventGeneric EventKind = "Generic"
)

type Cause struct {
	WatchName string
	Kind      CauseKind
	EventType EventKind

	Namespace string
	Name      string
	UID       types.UID

	OldResourceVersion string
	NewResourceVersion string

	OldGeneration int64
	NewGeneration int64

	GenerationChanged bool

	ChangedFields []string

	ObservedAt time.Time
}
