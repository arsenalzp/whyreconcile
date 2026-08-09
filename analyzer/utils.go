package analyzer

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/arsenalzp/whyreconcile/causes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func formatCauseSummary(summary map[causes.CauseKind]int) string {
	parts := make([]string, 0, len(summary))

	for kind, count := range summary {
		parts = append(parts, fmt.Sprintf("%s:%d", kind, count))
	}

	sort.Strings(parts)

	return strings.Join(parts, ", ")
}

func formatChangedFields(fields []causes.ChangedField) string {
	if len(fields) == 0 {
		return "-"
	}

	parts := make([]string, 0, len(fields))
	for _, f := range fields {
		parts = append(parts, string(f))
	}

	sort.Strings(parts)

	return strings.Join(parts, ",")
}

func deletionTimestampEqual(oldObj, newObj client.Object) bool {
	oldTS := oldObj.GetDeletionTimestamp()
	newTS := newObj.GetDeletionTimestamp()

	if oldTS == nil && newTS == nil {
		return true
	}

	if oldTS == nil || newTS == nil {
		return false
	}

	return oldTS.Equal(newTS)
}

func statusChanged(oldObj, newObj client.Object) bool {
	oldMap, ok := objectAsMap(oldObj)
	if !ok {
		return false
	}

	newMap, ok := objectAsMap(newObj)
	if !ok {
		return false
	}

	return !reflect.DeepEqual(oldMap["status"], newMap["status"])
}

func objectAsMap(obj client.Object) (map[string]any, bool) {
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, false
	}

	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, false
	}

	return m, true
}

func detectChangedFields(oldObj, newObj client.Object) []causes.ChangedField {
	fields := make([]causes.ChangedField, 0)

	if oldObj.GetName() != newObj.GetName() {
		fields = append(fields, causes.FieldMetadataName)
	}

	if oldObj.GetNamespace() != newObj.GetNamespace() {
		fields = append(fields, causes.FieldMetadataNamespace)
	}

	if oldObj.GetGeneration() != newObj.GetGeneration() {
		fields = append(fields, causes.FieldGeneration)
		fields = append(fields, causes.FieldSpec)
	}

	if !reflect.DeepEqual(oldObj.GetLabels(), newObj.GetLabels()) {
		fields = append(fields, causes.FieldMetadataLabels)
	}

	if !reflect.DeepEqual(oldObj.GetAnnotations(), newObj.GetAnnotations()) {
		fields = append(fields, causes.FieldMetadataAnnotations)
	}

	if !reflect.DeepEqual(oldObj.GetFinalizers(), newObj.GetFinalizers()) {
		fields = append(fields, causes.FieldMetadataFinalizers)
	}

	if !reflect.DeepEqual(oldObj.GetOwnerReferences(), newObj.GetOwnerReferences()) {
		fields = append(fields, causes.FieldMetadataOwnerRefs)
	}

	if !deletionTimestampEqual(oldObj, newObj) {
		fields = append(fields, causes.FieldMetadataDeletionTS)
	}

	if statusChanged(oldObj, newObj) {
		fields = append(fields, causes.FieldStatus)
	}

	return fields
}

func formatObjectRef(ref causes.ObjectRef) string {
	kind := ref.Kind
	if kind == "" {
		kind = "Object"
	}

	if ref.Namespace == "" {
		return fmt.Sprintf("%s/%s", kind, ref.Name)
	}

	return fmt.Sprintf("%s/%s/%s", kind, ref.Namespace, ref.Name)
}

func formatRequestRef(ref causes.RequestRef) string {
	if ref.Namespace == "" {
		return ref.Name
	}
	return ref.Namespace + "/" + ref.Name
}
