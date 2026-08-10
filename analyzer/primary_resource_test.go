package analyzer

import (
	"testing"

	"github.com/arsenalzp/whyreconcile/causes"
	"github.com/arsenalzp/whyreconcile/store"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestForPrimaryOptsUpdateRecordsPrimarySpecUpdate(t *testing.T) {
	s := store.NewCauseStore()
	scheme := mustScheme(t)

	a := NewAnalyzer("test-controller", s, false, scheme, logr.Discard())

	oldObj := newConfigMap("default", "sample", "10", 1)
	newObj := newConfigMap("default", "sample", "11", 2)

	p := ForPrimaryOpts{
		a:         a,
		watchName: "primary",
	}

	ok := p.Update(event.TypedUpdateEvent[client.Object]{
		ObjectOld: oldObj,
		ObjectNew: newObj,
	})

	if !ok {
		t.Fatalf("expected primary predicate to return true")
	}

	req := newRequest("default", "sample")

	got := s.Take(req)

	if len(got) != 1 {
		t.Fatalf("expected 1 cause, got %d", len(got))
	}

	cause := got[0]

	if cause.Kind != causes.CausePrimarySpecUpdate {
		t.Fatalf("expected cause kind %q, got %q", causes.CausePrimarySpecUpdate, cause.Kind)
	}

	if cause.EventType != causes.EventUpdate {
		t.Fatalf("expected event type %q, got %q", causes.EventUpdate, cause.EventType)
	}

	if !cause.GenerationChanged {
		t.Fatalf("expected GenerationChanged to be true")
	}

	if cause.Source.Name != "sample" {
		t.Fatalf("expected source name %q, got %q", "sample", cause.Source.Name)
	}

	if cause.Target.Namespace != "default" || cause.Target.Name != "sample" {
		t.Fatalf("expected target default/sample, got %v", cause.Target)
	}

	if cause.OldGeneration != 1 || cause.NewGeneration != 2 {
		t.Fatalf("expected generation 1->2, got %d->%d", cause.OldGeneration, cause.NewGeneration)
	}
}
