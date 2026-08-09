package analyzer

import (
	"context"
	"fmt"
	"time"

	"github.com/arsenalzp/whyreconcile/causes"
	"github.com/arsenalzp/whyreconcile/store"
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
	controllerName  string
	store           store.Store
	printEventTrace bool
	scheme          *runtime.Scheme
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
	rc.a.printReconsileTrace(req, causes)

	return rc.inner.Reconcile(ctx, req)
}

func (qw *QueueWrapper) Add(req reconcile.Request) {
	qw.store.Add(req, qw.cause)
	qw.inner.Add(req)
}

func (qw *QueueWrapper) AddAfter(req reconcile.Request, duration time.Duration) {
	qw.store.Add(req, qw.cause)
	qw.inner.AddAfter(req, duration)
}

func (qw *QueueWrapper) AddRateLimited(req reconcile.Request) {
	qw.store.Add(req, qw.cause)
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

func (a *Analyzer) NewForPrimaryOpts(watchName string, preds ...predicate.Predicate) builder.ForOption {
	p := ForPrimaryOpts{
		a:         a,
		watchName: watchName,
	}

	predicates := append(preds, p)

	return builder.WithPredicates(predicates...)
}

func (a *Analyzer) NewSecondaryHandler(watchName string, inner handler.TypedEventHandler[client.Object, reconcile.Request]) handler.TypedEventHandler[client.Object, reconcile.Request] {
	h := SecondaryResourceHandler{
		a:         a,
		watchName: watchName,
		inner:     inner,
	}

	return h
}

func (a *Analyzer) printReconsileTrace(req ctrl.Request, eventCauses []causes.Cause) {
	if len(eventCauses) == 0 {
		fmt.Printf(
			"[whyreconcile] controller=%s request=%s/%s causes=0 cause=%s\n",
			a.controllerName,
			req.Namespace,
			req.Name,
			causes.CausePrimaryUnknown,
		)

		return
	}

	summary := make(map[causes.CauseKind]int)

	for _, c := range eventCauses {
		summary[c.Kind]++
	}

	for _, c := range eventCauses {
		fmt.Printf(
			"[whyreconcile] controller=%s source=%s target=%s causes=%d summary=%v\n",
			a.controllerName,
			formatObjectRef(c.Source),
			formatRequestRef(c.Target),
			len(eventCauses),
			formatCauseSummary(summary),
		)
	}

	if !a.printEventTrace {
		return
	}

	for i, c := range eventCauses {
		fmt.Printf(
			"[whyreconcile] #%d watch=%s event=%s cause=%s source=%s target=%s namespace=%s name=%s resourceVersion=%s->%s generation=%d->%d observedAt=%s\n",
			i+1,
			c.WatchName,
			c.EventType,
			c.Kind,
			formatObjectRef(c.Source),
			formatRequestRef(c.Target),
			c.OldResourceVersion,
			c.NewResourceVersion,
			c.OldGeneration,
			c.NewGeneration,
			c.ObservedAt.Format(time.RFC3339Nano),
		)
	}
}

func NewAnalyzer(name string, s store.Store, printTrace bool, scheme *runtime.Scheme) *Analyzer {
	if s == nil {
		s = store.NewCauseStore()
	}

	return &Analyzer{
		controllerName:  name,
		store:           s,
		printEventTrace: printTrace,
		scheme:          scheme,
	}
}
