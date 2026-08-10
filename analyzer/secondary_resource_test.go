package analyzer

import (
	"context"
	"testing"

	"github.com/arsenalzp/whyreconcile/causes"
	"github.com/arsenalzp/whyreconcile/store"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestSecondaryUpdateStoresCauseUnderInnerHandlerRequest(t *testing.T) {
	s := store.NewCauseStore()
	scheme := mustScheme(t)

	a := NewAnalyzer("test-controller", s, false, scheme, logr.Discard())

	targetReq := newRequest("default", "owner-cr")

	inner := &fakeHandler{
		requests: []reconcile.Request{targetReq},
	}

	h := a.NewSecondaryHandler("owned-configmap", inner)

	q := &fakeQueue{}

	oldObj := newConfigMap("default", "owned-config", "10", 1)
	newObj := newConfigMap("default", "owned-config", "11", 1)

	h.Update(context.Background(), event.TypedUpdateEvent[client.Object]{
		ObjectOld: oldObj,
		ObjectNew: newObj,
	}, q)

	got := s.Take(targetReq)

	if len(got) != 1 {
		t.Fatalf("expected 1 cause for target request, got %d", len(got))
	}

	cause := got[0]

	if cause.Kind != causes.CauseSecondaryStatusOrMeta {
		t.Fatalf("expected cause kind %q, got %q", causes.CauseSecondaryStatusOrMeta, cause.Kind)
	}

	if cause.Source.Name != "owned-config" {
		t.Fatalf("expected source name %q, got %q", "owned-config", cause.Source.Name)
	}

	if cause.Target.Name != "owner-cr" {
		t.Fatalf("expected target name %q, got %q", "owner-cr", cause.Target.Name)
	}

	if cause.GenerationChanged {
		t.Fatalf("expected GenerationChanged to be false")
	}

	if len(q.added) != 1 {
		t.Fatalf("expected inner queue to receive 1 request, got %d", len(q.added))
	}
}
