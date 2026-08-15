package analyzer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/arsenalzp/whyreconcile/causes"
	"github.com/arsenalzp/whyreconcile/store"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestQueueWrapperAddEnrichesCauseWithTargetAndDelegates(t *testing.T) {
	s := store.NewCauseStore()
	q := &fakeQueue{}

	source := causes.ObjectRef{
		APIVersion: "v1",
		Kind:       "ConfigMap",
		Namespace:  "default",
		Name:       "source-config",
	}

	baseCause := causes.Cause{
		WatchName: "external-configmap",
		Kind:      causes.CauseExternalCreate,
		EventType: causes.EventCreate,
		Source:    source,
	}

	wrapped := WrapQueue(q, s, baseCause)

	req := newRequest("default", "target-cr")

	wrapped.Add(req)

	if len(q.added) != 1 {
		t.Fatalf("expected inner queue to receive 1 request, got %d", len(q.added))
	}

	if q.added[0] != req {
		t.Fatalf("expected inner queue request %v, got %v", req, q.added[0])
	}

	got := s.Take(req)

	if len(got) != 1 {
		t.Fatalf("expected 1 stored cause, got %d", len(got))
	}

	if got[0].Source != source {
		t.Fatalf("expected source %v, got %v", source, got[0].Source)
	}

	if got[0].Target.Namespace != req.Namespace {
		t.Fatalf("expected target namespace %q, got %q", req.Namespace, got[0].Target.Namespace)
	}

	if got[0].Target.Name != req.Name {
		t.Fatalf("expected target name %q, got %q", req.Name, got[0].Target.Name)
	}

	if wrapped.cause.Target.Namespace != "" || wrapped.cause.Target.Name != "" {
		t.Fatalf("expected base cause target to remain empty, got %v", wrapped.cause.Target)
	}
}

func TestReconcileWrapperConsumesCausesAndCallsInner(t *testing.T) {
	s := store.NewCauseStore()
	scheme := mustScheme(t)

	a := NewAnalyzer("test-controller", s, false, scheme, logr.Discard())

	req := newRequest("default", "sample")

	s.Add(req, causes.Cause{
		WatchName: "primary",
		Kind:      causes.CausePrimaryCreate,
		EventType: causes.EventCreate,
		Target: causes.RequestRef{
			Namespace: req.Namespace,
			Name:      req.Name,
		},
	})

	inner := &fakeReconciler{}

	wrapped := a.WrapReconcile(inner)

	_, err := wrapped.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}

	if !inner.called {
		t.Fatalf("expected inner reconciler to be called")
	}

	if inner.req != req {
		t.Fatalf("expected inner reconciler request %v, got %v", req, inner.req)
	}

	left := s.Take(req)
	if len(left) != 0 {
		t.Fatalf("expected causes to be consumed, got %d left", len(left))
	}
}

func TestObjectRefResolvesGVKFromScheme(t *testing.T) {
	scheme := mustScheme(t)

	a := NewAnalyzer("test-controller", store.NewCauseStore(), false, scheme, logr.Discard())

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "sample",
			UID:       types.UID("uid-1"),
		},
	}

	ref := a.objectRef(cm)

	if ref.APIVersion != "v1" {
		t.Fatalf("expected apiVersion %q, got %q", "v1", ref.APIVersion)
	}

	if ref.Kind != "ConfigMap" {
		t.Fatalf("expected kind %q, got %q", "ConfigMap", ref.Kind)
	}

	if ref.Namespace != "default" {
		t.Fatalf("expected namespace %q, got %q", "default", ref.Namespace)
	}

	if ref.Name != "sample" {
		t.Fatalf("expected name %q, got %q", "sample", ref.Name)
	}

	if ref.UID != types.UID("uid-1") {
		t.Fatalf("expected uid %q, got %q", types.UID("uid-1"), ref.UID)
	}
}

func TestReconcileWrapperRecordsErrorRetryCause(t *testing.T) {
	causeStore := store.NewCauseStore()

	analyzer := NewAnalyzer(
		"test-controller",
		causeStore,
		false,
		runtime.NewScheme(),
		logr.Discard(),
	)

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: "default",
			Name:      "sample",
		},
	}

	inner := &derivedCauseReconciler{
		result: ctrl.Result{},
		err:    errors.New("boom"),
	}

	wrapped := analyzer.WrapReconcile(inner)

	_, err := wrapped.Reconcile(context.Background(), req)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	got := causeStore.Take(req)

	if len(got) != 1 {
		t.Fatalf("expected 1 reconcile-derived cause, got %d", len(got))
	}

	cause := got[0]

	if cause.Kind != causes.CauseReconcileErrorRetry {
		t.Fatalf("expected cause kind %q, got %q", causes.CauseReconcileErrorRetry, cause.Kind)
	}

	if cause.EventType != causes.EventReconcileError {
		t.Fatalf("expected event type %q, got %q", causes.EventReconcileError, cause.EventType)
	}

	if cause.WatchName != "reconcile" {
		t.Fatalf("expected watch name %q, got %q", "reconcile", cause.WatchName)
	}

	if cause.Error != "boom" {
		t.Fatalf("expected error %q, got %q", "boom", cause.Error)
	}

	if cause.Target.Namespace != req.Namespace {
		t.Fatalf("expected target namespace %q, got %q", req.Namespace, cause.Target.Namespace)
	}

	if cause.Target.Name != req.Name {
		t.Fatalf("expected target name %q, got %q", req.Name, cause.Target.Name)
	}
}

func TestReconcileWrapperRecordsRequeueAfterCause(t *testing.T) {
	causeStore := store.NewCauseStore()

	analyzer := NewAnalyzer(
		"test-controller",
		causeStore,
		false,
		runtime.NewScheme(),
		logr.Discard(),
	)

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: "default",
			Name:      "sample",
		},
	}

	inner := &derivedCauseReconciler{
		result: ctrl.Result{
			RequeueAfter: time.Minute,
		},
		err: nil,
	}

	wrapped := analyzer.WrapReconcile(inner)

	result, err := wrapped.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.RequeueAfter != time.Minute {
		t.Fatalf("expected RequeueAfter %s, got %s", time.Minute, result.RequeueAfter)
	}

	got := causeStore.Take(req)

	if len(got) != 1 {
		t.Fatalf("expected 1 reconcile-derived cause, got %d", len(got))
	}

	cause := got[0]

	if cause.Kind != causes.CauseReconcileRequeueAfter {
		t.Fatalf("expected cause kind %q, got %q", causes.CauseReconcileRequeueAfter, cause.Kind)
	}

	if cause.EventType != causes.EventRequeueAfter {
		t.Fatalf("expected event type %q, got %q", causes.EventRequeueAfter, cause.EventType)
	}

	if cause.WatchName != "reconcile" {
		t.Fatalf("expected watch name %q, got %q", "reconcile", cause.WatchName)
	}

	if cause.RequeueAfter != time.Minute {
		t.Fatalf("expected RequeueAfter %s, got %s", time.Minute, cause.RequeueAfter)
	}

	if cause.Target.Namespace != req.Namespace {
		t.Fatalf("expected target namespace %q, got %q", req.Namespace, cause.Target.Namespace)
	}

	if cause.Target.Name != req.Name {
		t.Fatalf("expected target name %q, got %q", req.Name, cause.Target.Name)
	}
}

func TestReconcileWrapperDoesNotRecordDerivedCauseForNormalResult(t *testing.T) {
	causeStore := store.NewCauseStore()

	analyzer := NewAnalyzer(
		"test-controller",
		causeStore,
		false,
		runtime.NewScheme(),
		logr.Discard(),
	)

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: "default",
			Name:      "sample",
		},
	}

	inner := &derivedCauseReconciler{
		result: ctrl.Result{},
		err:    nil,
	}

	wrapped := analyzer.WrapReconcile(inner)

	result, err := wrapped.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result != (ctrl.Result{}) {
		t.Fatalf("expected empty result, got %v", result)
	}

	got := causeStore.Take(req)

	if len(got) != 0 {
		t.Fatalf("expected 0 reconcile-derived causes, got %d", len(got))
	}
}

type derivedCauseReconciler struct {
	result ctrl.Result
	err    error
}

func (r *derivedCauseReconciler) Reconcile(
	ctx context.Context,
	req reconcile.Request,
) (reconcile.Result, error) {
	return r.result, r.err
}

func newRequest(namespace, name string) reconcile.Request {
	return reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		},
	}
}

func newConfigMap(namespace, name, resourceVersion string, generation int64) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       namespace,
			Name:            name,
			ResourceVersion: resourceVersion,
			Generation:      generation,
			UID:             types.UID(namespace + "-" + name),
		},
	}
}

func mustScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()

	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 to scheme: %v", err)
	}

	return scheme
}

type fakeReconciler struct {
	called bool
	req    reconcile.Request
}

func (r *fakeReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	r.called = true
	r.req = req

	return reconcile.Result{}, nil
}

type fakeHandler struct {
	requests []reconcile.Request
}

var _ handler.TypedEventHandler[client.Object, reconcile.Request] = (*fakeHandler)(nil)

func (h *fakeHandler) Create(
	ctx context.Context,
	e event.TypedCreateEvent[client.Object],
	q workqueue.TypedRateLimitingInterface[reconcile.Request],
) {
	for _, req := range h.requests {
		q.Add(req)
	}
}

func (h *fakeHandler) Update(
	ctx context.Context,
	e event.TypedUpdateEvent[client.Object],
	q workqueue.TypedRateLimitingInterface[reconcile.Request],
) {
	for _, req := range h.requests {
		q.Add(req)
	}
}

func (h *fakeHandler) Delete(
	ctx context.Context,
	e event.TypedDeleteEvent[client.Object],
	q workqueue.TypedRateLimitingInterface[reconcile.Request],
) {
	for _, req := range h.requests {
		q.Add(req)
	}
}

func (h *fakeHandler) Generic(
	ctx context.Context,
	e event.TypedGenericEvent[client.Object],
	q workqueue.TypedRateLimitingInterface[reconcile.Request],
) {
	for _, req := range h.requests {
		q.Add(req)
	}
}

type fakeQueue struct {
	added            []reconcile.Request
	addedAfter       []reconcile.Request
	addedRateLimited []reconcile.Request
	shuttingDown     bool
}

var _ workqueue.TypedRateLimitingInterface[reconcile.Request] = (*fakeQueue)(nil)

func (q *fakeQueue) Add(req reconcile.Request) {
	q.added = append(q.added, req)
}

func (q *fakeQueue) AddAfter(req reconcile.Request, duration time.Duration) {
	q.addedAfter = append(q.addedAfter, req)
}

func (q *fakeQueue) AddRateLimited(req reconcile.Request) {
	q.addedRateLimited = append(q.addedRateLimited, req)
}

func (q *fakeQueue) Len() int {
	return len(q.added)
}

func (q *fakeQueue) Get() (reconcile.Request, bool) {
	if len(q.added) == 0 {
		return reconcile.Request{}, q.shuttingDown
	}

	req := q.added[0]
	q.added = q.added[1:]

	return req, q.shuttingDown
}

func (q *fakeQueue) Done(req reconcile.Request) {}

func (q *fakeQueue) ShutDown() {
	q.shuttingDown = true
}

func (q *fakeQueue) ShutDownWithDrain() {
	q.shuttingDown = true
}

func (q *fakeQueue) ShuttingDown() bool {
	return q.shuttingDown
}

func (q *fakeQueue) Forget(req reconcile.Request) {}

func (q *fakeQueue) NumRequeues(req reconcile.Request) int {
	return 0
}
