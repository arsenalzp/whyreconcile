package analyzer

import (
	"testing"

	"github.com/arsenalzp/whyreconcile/causes"
)

func TestFormatCauseSummaryStableOrder(t *testing.T) {
	summary := map[causes.CauseKind]int{
		causes.CauseSecondaryStatusOrMeta: 2,
		causes.CausePrimaryCreate:         1,
		causes.CauseExternalSpecUpdate:    3,
	}

	got := formatCauseSummary(summary)

	expected := "ExternalResourceSpecUpdate:3, PrimaryResourceCreate:1, SecondaryResourceStatusOrMetadata:2"

	if got != expected {
		t.Fatalf("expected summary %q, got %q", expected, got)
	}
}

func TestDetectChangedFieldsGenerationLabelsAnnotations(t *testing.T) {
	oldObj := newConfigMap("default", "sample", "10", 1)
	newObj := newConfigMap("default", "sample", "11", 2)

	oldObj.Labels = map[string]string{
		"app": "old",
	}
	newObj.Labels = map[string]string{
		"app": "new",
	}

	oldObj.Annotations = map[string]string{
		"checksum": "old",
	}
	newObj.Annotations = map[string]string{
		"checksum": "new",
	}

	got := detectChangedFields(oldObj, newObj)

	expected := []causes.ChangedField{
		causes.FieldGeneration,
		causes.FieldSpec,
		causes.FieldMetadataLabels,
		causes.FieldMetadataAnnotations,
	}

	for _, field := range expected {
		if !containsChangedField(got, field) {
			t.Fatalf("expected changed fields %v to contain %q", got, field)
		}
	}
}

func containsChangedField(fields []causes.ChangedField, expected causes.ChangedField) bool {
	for _, field := range fields {
		if field == expected {
			return true
		}
	}

	return false
}
