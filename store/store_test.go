package store

import (
	"fmt"
	"sync"
	"testing"

	"github.com/arsenalzp/whyreconcile/causes"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestCauseStoreImplementsStore(t *testing.T) {
	var _ Store = (*CauseStore)(nil)
}

func TestCauseStoreAddAndTake(t *testing.T) {
	s := NewCauseStore()

	req := newRequest("default", "sample")

	cause := causes.Cause{
		WatchName: "primary",
		Kind:      causes.CausePrimaryCreate,
		EventType: causes.EventCreate,
		Target: causes.RequestRef{
			Namespace: "default",
			Name:      "sample",
		},
	}

	s.Add(req, cause)

	got := s.Take(req)

	if len(got) != 1 {
		t.Fatalf("expected 1 cause, got %d", len(got))
	}

	if got[0].Kind != causes.CausePrimaryCreate {
		t.Fatalf("expected cause kind %q, got %q", causes.CausePrimaryCreate, got[0].Kind)
	}

	if got[0].WatchName != "primary" {
		t.Fatalf("expected watch name %q, got %q", "primary", got[0].WatchName)
	}

	if got[0].EventType != causes.EventCreate {
		t.Fatalf("expected event type %q, got %q", causes.EventCreate, got[0].EventType)
	}

	if got[0].Target.Namespace != "default" {
		t.Fatalf("expected target namespace %q, got %q", "default", got[0].Target.Namespace)
	}

	if got[0].Target.Name != "sample" {
		t.Fatalf("expected target name %q, got %q", "sample", got[0].Target.Name)
	}
}

func TestCauseStoreAddAppendsMultipleCausesForSameRequest(t *testing.T) {
	s := NewCauseStore()

	req := newRequest("default", "sample")

	cause1 := causes.Cause{
		WatchName: "primary",
		Kind:      causes.CausePrimaryCreate,
		EventType: causes.EventCreate,
	}

	cause2 := causes.Cause{
		WatchName: "primary",
		Kind:      causes.CausePrimarySpecUpdate,
		EventType: causes.EventUpdate,
	}

	s.Add(req, cause1)
	s.Add(req, cause2)

	got := s.Take(req)

	if len(got) != 2 {
		t.Fatalf("expected 2 causes, got %d", len(got))
	}

	if got[0].Kind != causes.CausePrimaryCreate {
		t.Fatalf("expected first cause kind %q, got %q", causes.CausePrimaryCreate, got[0].Kind)
	}

	if got[1].Kind != causes.CausePrimarySpecUpdate {
		t.Fatalf("expected second cause kind %q, got %q", causes.CausePrimarySpecUpdate, got[1].Kind)
	}
}

func TestCauseStoreTakeDeletesCauses(t *testing.T) {
	s := NewCauseStore()

	req := newRequest("default", "sample")

	cause := causes.Cause{
		WatchName: "primary",
		Kind:      causes.CausePrimaryCreate,
		EventType: causes.EventCreate,
	}

	s.Add(req, cause)

	first := s.Take(req)
	if len(first) != 1 {
		t.Fatalf("expected first Take to return 1 cause, got %d", len(first))
	}

	second := s.Take(req)
	if len(second) != 0 {
		t.Fatalf("expected second Take to return 0 causes, got %d", len(second))
	}
}

func TestCauseStoreKeepsRequestsSeparated(t *testing.T) {
	s := NewCauseStore()

	reqA := newRequest("default", "sample-a")
	reqB := newRequest("default", "sample-b")

	causeA := causes.Cause{
		WatchName: "primary",
		Kind:      causes.CausePrimaryCreate,
		EventType: causes.EventCreate,
		Target: causes.RequestRef{
			Namespace: reqA.Namespace,
			Name:      reqA.Name,
		},
	}

	causeB := causes.Cause{
		WatchName: "secondary",
		Kind:      causes.CauseSecondaryCreate,
		EventType: causes.EventCreate,
		Target: causes.RequestRef{
			Namespace: reqB.Namespace,
			Name:      reqB.Name,
		},
	}

	s.Add(reqA, causeA)
	s.Add(reqB, causeB)

	gotA := s.Take(reqA)
	if len(gotA) != 1 {
		t.Fatalf("expected 1 cause for reqA, got %d", len(gotA))
	}

	if gotA[0].Kind != causes.CausePrimaryCreate {
		t.Fatalf("expected reqA cause kind %q, got %q", causes.CausePrimaryCreate, gotA[0].Kind)
	}

	if gotA[0].Target.Namespace != "default" {
		t.Fatalf("expected reqA target namespace %q, got %q", "default", gotA[0].Target.Namespace)
	}

	if gotA[0].Target.Name != "sample-a" {
		t.Fatalf("expected reqA target name %q, got %q", "sample-a", gotA[0].Target.Name)
	}

	gotB := s.Take(reqB)
	if len(gotB) != 1 {
		t.Fatalf("expected 1 cause for reqB, got %d", len(gotB))
	}

	if gotB[0].Kind != causes.CauseSecondaryCreate {
		t.Fatalf("expected reqB cause kind %q, got %q", causes.CauseSecondaryCreate, gotB[0].Kind)
	}

	if gotB[0].Target.Namespace != "default" {
		t.Fatalf("expected reqB target namespace %q, got %q", "default", gotB[0].Target.Namespace)
	}

	if gotB[0].Target.Name != "sample-b" {
		t.Fatalf("expected reqB target name %q, got %q", "sample-b", gotB[0].Target.Name)
	}
}

func TestCauseStoreTakeUnknownRequestReturnsEmptySlice(t *testing.T) {
	s := NewCauseStore()

	req := newRequest("default", "missing")

	got := s.Take(req)

	if len(got) != 0 {
		t.Fatalf("expected 0 causes for unknown request, got %d", len(got))
	}
}

func TestCauseStoreConcurrentAddSameRequest(t *testing.T) {
	s := NewCauseStore()

	req := newRequest("default", "sample")

	const workers = 20
	const causesPerWorker = 100

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < causesPerWorker; j++ {
				s.Add(req, causes.Cause{
					WatchName: "primary",
					Kind:      causes.CausePrimarySpecUpdate,
					EventType: causes.EventUpdate,
					Target: causes.RequestRef{
						Namespace: req.Namespace,
						Name:      req.Name,
					},
				})
			}
		}()
	}

	wg.Wait()

	got := s.Take(req)

	expected := workers * causesPerWorker
	if len(got) != expected {
		t.Fatalf("expected %d causes, got %d", expected, len(got))
	}
}

func TestCauseStoreConcurrentAddDifferentRequests(t *testing.T) {
	s := NewCauseStore()

	const workers = 20
	const causesPerWorker = 50

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			req := newRequest("default", fmt.Sprintf("sample-%d", workerID))

			for j := 0; j < causesPerWorker; j++ {
				s.Add(req, causes.Cause{
					WatchName: "primary",
					Kind:      causes.CausePrimarySpecUpdate,
					EventType: causes.EventUpdate,
					Target: causes.RequestRef{
						Namespace: req.Namespace,
						Name:      req.Name,
					},
				})
			}
		}(i)
	}

	wg.Wait()

	for i := 0; i < workers; i++ {
		req := newRequest("default", fmt.Sprintf("sample-%d", i))

		got := s.Take(req)

		if len(got) != causesPerWorker {
			t.Fatalf(
				"expected %d causes for req %s/%s, got %d",
				causesPerWorker,
				req.Namespace,
				req.Name,
				len(got),
			)
		}

		for _, cause := range got {
			if cause.Target.Namespace != req.Namespace {
				t.Fatalf("expected target namespace %q, got %q", req.Namespace, cause.Target.Namespace)
			}

			if cause.Target.Name != req.Name {
				t.Fatalf("expected target name %q, got %q", req.Name, cause.Target.Name)
			}
		}
	}
}

func TestCauseStoreTakeAfterConcurrentAddDeletesCauses(t *testing.T) {
	s := NewCauseStore()

	req := newRequest("default", "sample")

	const causesCount = 100

	var wg sync.WaitGroup

	for i := 0; i < causesCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			s.Add(req, causes.Cause{
				WatchName: "primary",
				Kind:      causes.CausePrimarySpecUpdate,
				EventType: causes.EventUpdate,
			})
		}()
	}

	wg.Wait()

	first := s.Take(req)
	if len(first) != causesCount {
		t.Fatalf("expected first Take to return %d causes, got %d", causesCount, len(first))
	}

	second := s.Take(req)
	if len(second) != 0 {
		t.Fatalf("expected second Take to return 0 causes, got %d", len(second))
	}
}

func newRequest(namespace, name string) reconcile.Request {
	return reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		},
	}
}
