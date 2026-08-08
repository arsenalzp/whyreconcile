package causes

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

type CauseKind = string
type EventKind = string
type ChangedField = string

const (
	CausePrimaryCreate       CauseKind = "PrimaryResourceCreate"
	CausePrimarySpecUpdate   CauseKind = "PrimaryResourceSpecUpdate"
	CausePrimaryStatusOrMeta CauseKind = "PrimaryResourceStatusOrMetadata"
	CausePrimaryDelete       CauseKind = "PrimaryResourceDelete"
	CausePrimaryGeneric      CauseKind = "PrimaryResourceGeneric"
	CausePrimaryUnknown      CauseKind = "PrimaryResourceUnknownOrRequeue"

	CauseSecondaryCreate       CauseKind = "SecondaryResourceCreate"
	CauseSecondarySpecUpdate   CauseKind = "SecondaryResourceSpecUpdate"
	CauseSecondaryStatusOrMeta CauseKind = "SecondaryResourceStatusOrMetadata"
	CauseSecondaryDelete       CauseKind = "SecondaryResourceDelete"
	CauseSecondaryGeneric      CauseKind = "SecondaryResourceGeneric"
	CauseSecondaryUnknown      CauseKind = "SecondaryResourceUnknownOrRequeue"

	CauseExternalCreate       CauseKind = "ExternalResourceCreate"
	CauseExternalSpecUpdate   CauseKind = "ExternalResourceSpecUpdate"
	CauseExternalStatusOrMeta CauseKind = "ExternalResourceStatusOrMetadata"
	CauseExternalDelete       CauseKind = "ExternalResourceDelete"
	CauseExternalGeneric      CauseKind = "ExternalResourceGeneric"
	CauseExternalUnknown      CauseKind = "ExternalResourceUnknownOrRequeue"

	EventCreate  EventKind = "Create"
	EventUpdate  EventKind = "Update"
	EventDelete  EventKind = "Delete"
	EventGeneric EventKind = "Generic"

	FieldSpec   ChangedField = "spec"
	FieldStatus ChangedField = "status"

	FieldMetadataName        ChangedField = "metadata.name"
	FieldMetadataNamespace   ChangedField = "metadata.namespace"
	FieldMetadataLabels      ChangedField = "metadata.labels"
	FieldMetadataAnnotations ChangedField = "metadata.annotations"
	FieldMetadataFinalizers  ChangedField = "metadata.finalizers"
	FieldMetadataOwnerRefs   ChangedField = "metadata.ownerReferences"
	FieldMetadataDeletionTS  ChangedField = "metadata.deletionTimestamp"
	FieldGeneration          ChangedField = "metadata.generation"
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

func (c Cause) PrintTraceCreate() {
	fmt.Printf(
		"[whyreconcile] watch=%s event=%s namespace=%s name=%s resourceVersion=%s generation=%d\n",
		c.WatchName,
		c.Kind,
		c.Namespace,
		c.Name,
		c.NewResourceVersion,
		c.NewGeneration,
	)
}

func (c Cause) PrintTraceDelete() {
	fmt.Printf(
		"[whyreconcile] watch=%s event=%s namespace=%s name=%s resourceVersion=%s generation=%d\n",
		c.WatchName,
		c.Kind,
		c.Namespace,
		c.Name,
		c.NewResourceVersion,
		c.NewGeneration,
	)
}

func (c Cause) PrintTraceUpdate() {
	// Check wheather an object generation was changed, true or false
	generationChanged := c.OldGeneration != c.NewGeneration

	fmt.Printf(
		"[whyreconcile] watch=%s event=%s cause=%s namespace=%s name=%s resourceVersion=%s->%s generation=%d->%d generationChanged=%t\n",
		c.WatchName,
		c.EventType,
		c.Kind,
		c.Namespace,
		c.Name,
		c.OldResourceVersion,
		c.NewResourceVersion,
		c.OldGeneration,
		c.NewGeneration,
		generationChanged,
	)
}

func (c Cause) PrintTraceGeneric() {
	fmt.Printf(
		"[whyreconcile] watch=%s event=%s namespace=%s name=%s resourceVersion=%s generation=%d\n",
		c.WatchName,
		c.Kind,
		c.Namespace,
		c.Name,
		c.NewResourceVersion,
		c.NewGeneration,
	)
}
