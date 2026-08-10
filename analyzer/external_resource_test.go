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

func TestExternalUpdateStoresCauseForMultipleMappedRequests(t *testing.T) {
	s := store.NewCauseStore()
	scheme := mustScheme(t)

	a := NewAnalyzer("test-controller", s, false, scheme, logr.Discard())

	reqA := newRequest("default", "app-a")
	reqB := newRequest("default", "app-b")

	inner := &fakeHandler{
		requests: []reconcile.Request{reqA, reqB},
	}

	h := a.NewExternalHandler("external-configmap", inner)

	q := &fakeQueue{}

	oldObj := newConfigMap("default", "shared-config", "10", 1)
	newObj := newConfigMap("default", "shared-config", "11", 2)

	h.Update(context.Background(), event.TypedUpdateEvent[client.Object]{
		ObjectOld: oldObj,
		ObjectNew: newObj,
	}, q)

	gotA := s.Take(reqA)
	if len(gotA) != 1 {
		t.Fatalf("expected 1 cause for reqA, got %d", len(gotA))
	}

	if gotA[0].Kind != causes.CauseExternalSpecUpdate {
		t.Fatalf("expected reqA cause kind %q, got %q", causes.CauseExternalSpecUpdate, gotA[0].Kind)
	}

	if gotA[0].Source.Name != "shared-config" {
		t.Fatalf("expected reqA source name %q, got %q", "shared-config", gotA[0].Source.Name)
	}

	if gotA[0].Target.Name != "app-a" {
		t.Fatalf("expected reqA target name %q, got %q", "app-a", gotA[0].Target.Name)
	}

	gotB := s.Take(reqB)
	if len(gotB) != 1 {
		t.Fatalf("expected 1 cause for reqB, got %d", len(gotB))
	}

	if gotB[0].Kind != causes.CauseExternalSpecUpdate {
		t.Fatalf("expected reqB cause kind %q, got %q", causes.CauseExternalSpecUpdate, gotB[0].Kind)
	}

	if gotB[0].Source.Name != "shared-config" {
		t.Fatalf("expected reqB source name %q, got %q", "shared-config", gotB[0].Source.Name)
	}

	if gotB[0].Target.Name != "app-b" {
		t.Fatalf("expected reqB target name %q, got %q", "app-b", gotB[0].Target.Name)
	}

	if len(q.added) != 2 {
		t.Fatalf("expected inner queue to receive 2 requests, got %d", len(q.added))
	}
}
