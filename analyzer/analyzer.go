package analyzer

import (
	"context"
	"time"

	"github.com/arsenalzp/whyreconcile/causes"
	"github.com/arsenalzp/whyreconcile/store"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type Analyzer struct {
	controllerName    string
	store             store.Store
	printCauseDetails bool
	scheme            *runtime.Scheme
	log               logr.Logger
}

// This type is used to hold original Reconciler
type ReconcileWrapper struct {
	a     *Analyzer
	inner reconcile.Reconciler
}

// This type is used to hold original WorkQueue
type QueueWrapper struct {
	inner workqueue.TypedRateLimitingInterface[reconcile.Request]
	store store.Store
	cause causes.Cause
}

// Create a new wrapper around the original work queue
func WrapQueue(
	inner workqueue.TypedRateLimitingInterface[reconcile.Request],
	s store.Store,
	cause causes.Cause) *QueueWrapper {
	return &QueueWrapper{
		inner: inner,
		store: s,
		cause: cause,
	}
}

// Create a new wrapper around the original Reconciler
func (a *Analyzer) WrapReconcile(r reconcile.Reconciler) reconcile.Reconciler {
	wrapper := ReconcileWrapper{
		a:     a,
		inner: r,
	}

	return &wrapper
}

// Reconciler wrapper around the original Reconciler
func (rc *ReconcileWrapper) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	causes := rc.a.store.Take(req)
	rc.a.printReconcileTrace(req, causes)

	return rc.inner.Reconcile(ctx, req)
}

func (qw *QueueWrapper) enrichCause(cause causes.Cause, req reconcile.Request) causes.Cause {
	c := cause
	c.Target = causes.RequestRef{
		Namespace: req.Namespace,
		Name:      req.Name,
	}

	return c
}

func (qw *QueueWrapper) Add(req reconcile.Request) {
	qw.store.Add(req, qw.enrichCause(qw.cause, req))
	qw.inner.Add(req)
}

func (qw *QueueWrapper) AddAfter(req reconcile.Request, duration time.Duration) {
	qw.store.Add(req, qw.enrichCause(qw.cause, req))
	qw.inner.AddAfter(req, duration)
}

func (qw *QueueWrapper) AddRateLimited(req reconcile.Request) {
	qw.store.Add(req, qw.enrichCause(qw.cause, req))
	qw.inner.AddRateLimited(req)
}

func (qw *QueueWrapper) Len() int {
	return qw.inner.Len()
}
func (qw *QueueWrapper) Get() (reconcile.Request, bool) {
	return qw.inner.Get()
}
func (qw *QueueWrapper) Done(req reconcile.Request) {
	qw.inner.Done(req)
}
func (qw *QueueWrapper) ShutDown() {
	qw.inner.ShutDown()
}

func (qw *QueueWrapper) ShutDownWithDrain() {
	qw.inner.ShutDownWithDrain()
}

func (qw *QueueWrapper) ShuttingDown() bool {
	return qw.inner.ShuttingDown()
}

func (qw *QueueWrapper) Forget(req reconcile.Request) {
	qw.inner.Forget(req)
}

func (qw *QueueWrapper) NumRequeues(req reconcile.Request) int {
	return qw.inner.NumRequeues(req)
}

// Marshall an object into ObjectRef structure.
// If the object's GVK is empty then get the necessary information
// from the runtime Scheme
func (a *Analyzer) objectRef(obj client.Object) causes.ObjectRef {
	gvk := obj.GetObjectKind().GroupVersionKind()

	if gvk.Empty() && a.scheme != nil {
		resolvedGVK, err := apiutil.GVKForObject(obj, a.scheme)
		if err == nil {
			gvk = resolvedGVK
		}
	}

	return causes.ObjectRef{
		APIVersion: gvk.GroupVersion().String(),
		Kind:       gvk.Kind,
		Namespace:  obj.GetNamespace(),
		Name:       obj.GetName(),
		UID:        obj.GetUID(),
	}
}

// Creates a new predicates wrapper for Primary resource.
// The term Primary resource is used according to Kubebuilder and Operator SDK documentation.
// The wrapper is used along For() builder's method.
func (a *Analyzer) NewPrimaryOpts(watchName string, preds ...predicate.Predicate) builder.ForOption {
	p := ForPrimaryOpts{
		a:         a,
		watchName: watchName,
	}

	predicates := append(preds, p)

	return builder.WithPredicates(predicates...)
}

// Creates a new handler wrapper for Secondary (owned) resource
// The term Secondary resource is used according to Kubebuilder and Operator SDK documentation.
// The handler is used along Watches() builder's method, which represents Owns() builder's method.
func (a *Analyzer) NewSecondaryHandler(watchName string, inner handler.TypedEventHandler[client.Object, reconcile.Request]) handler.TypedEventHandler[client.Object, reconcile.Request] {
	h := SecondaryResourceHandler{
		a:         a,
		watchName: watchName,
		inner:     inner,
	}

	return h
}

// Creates a new handler wrapper for External resource.
// The term External resource is used according to Kubebuilder and Operator SDK documentation.
// The handler is used along Watches() builder's method.
func (a *Analyzer) NewExternalHandler(watchName string, inner handler.TypedEventHandler[client.Object, reconcile.Request]) handler.TypedEventHandler[client.Object, reconcile.Request] {
	h := ExternalResourceHandler{
		a:         a,
		watchName: watchName,
		inner:     inner,
	}

	return h
}

func (a *Analyzer) printReconcileTrace(req ctrl.Request, eventCauses []causes.Cause) {
	target := causes.RequestRef{
		Namespace: req.Namespace,
		Name:      req.Name,
	}

	// Print the details when no cause found.
	// Usually it means that the request was intentionally enqueued.
	if len(eventCauses) == 0 {
		a.log.Info(
			"reconcile cause",
			"controller", a.controllerName,
			"target", formatRequestRef(target),
			"causes", 0,
			"cause", causes.CauseUnknownOrRequeue,
		)

		return
	}

	summary := make(map[causes.CauseKind]int)
	for _, c := range eventCauses {
		summary[c.Kind]++
	}

	a.log.Info(
		"reconcile causes",
		"controller", a.controllerName,
		"target", formatRequestRef(target),
		"causes", len(eventCauses),
		"summary", formatCauseSummary(summary),
	)

	if !a.printCauseDetails {
		return
	}

	for i, c := range eventCauses {
		a.log.Info(
			"reconcile cause detail",
			"index", i+1,
			"watch", c.WatchName,
			"event", c.EventType,
			"cause", c.Kind,
			"source", formatObjectRef(c.Source),
			"target", formatRequestRef(c.Target),
			"oldResourceVersion", c.OldResourceVersion,
			"newResourceVersion", c.NewResourceVersion,
			"oldGeneration", c.OldGeneration,
			"newGeneration", c.NewGeneration,
			"changedFields", c.ChangedFields,
			"observedAt", c.ObservedAt.Format(time.RFC3339Nano),
		)
	}
}

// Creates a new instance of whyreconcile analyzer with the given name
// It is possible to use your own reconcilation cause store, just implement the Store interface
// with thread-safe methods.
// To print out changed fields use printCauseDetails parameter, it shows which fields were changed therefore could
// enqueue reconcile requests.
// The following fields are compared, however the list could be extended easily:
// metadata.name
// metadata.namespace
// metadata.labels
// metadata.annotations
// metadata.finalizers
// metadata.ownerReferences
// metadata.deletionTimestamp
// metadata.generation
func NewAnalyzer(name string, s store.Store, printCauseDetails bool, scheme *runtime.Scheme, log logr.Logger) *Analyzer {
	if s == nil {
		s = store.NewCauseStore()
	}

	if log.GetSink() == nil {
		log = logr.Logger{} // this expression is equal to logr.Discard()
	}

	return &Analyzer{
		controllerName:    name,
		store:             s,
		printCauseDetails: printCauseDetails,
		scheme:            scheme,
		log:               log,
	}
}
