package handlers

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/arsenalzp/whyreconcile/causes"
	"github.com/arsenalzp/whyreconcile/store"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type Analyzer struct {
	controllerName  string
	store           store.Store
	printEventTrace bool
}

type ForPrimaryOpts struct {
	a         *Analyzer
	watchName string
}

type ReconcileWrapper struct {
	a     *Analyzer
	inner reconcile.Reconciler
}

func (fpo ForPrimaryOpts) Create(e event.TypedCreateEvent[client.Object]) bool {
	watch := fpo.watchName
	event := causes.EventCreate
	namespace := e.Object.GetNamespace()
	name := e.Object.GetName()
	resourceVersion := e.Object.GetResourceVersion()
	generation := e.Object.GetGeneration()
	uid := e.Object.GetUID()

	if fpo.a.printEventTrace {
		fmt.Printf(
			"[whyreconcile] watch=%s event=%s namespace=%s name=%s resourceVersion=%s generation=%d\n",
			watch,
			event,
			namespace,
			name,
			resourceVersion,
			generation,
		)
	}

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		},
	}

	fpo.a.store.Add(req, causes.Cause{
		WatchName: watch,
		Kind:      causes.CausePrimaryCreate,
		EventType: event,

		Namespace: namespace,
		Name:      name,
		UID:       uid,

		NewResourceVersion: resourceVersion,
		NewGeneration:      generation,

		ObservedAt: time.Now(),
	})

	return true
}

func (fpo ForPrimaryOpts) Delete(e event.TypedDeleteEvent[client.Object]) bool {
	watch := fpo.watchName
	event := causes.EventDelete
	namespace := e.Object.GetNamespace()
	name := e.Object.GetName()
	resourceVersion := e.Object.GetResourceVersion()
	generation := e.Object.GetGeneration()
	uid := e.Object.GetUID()

	if fpo.a.printEventTrace {
		fmt.Printf(
			"[whyreconcile] watch=%s event=%s namespace=%s name=%s resourceVersion=%s generation=%d\n",
			watch,
			event,
			namespace,
			name,
			resourceVersion,
			generation,
		)
	}

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		},
	}

	fpo.a.store.Add(req, causes.Cause{
		WatchName: watch,
		Kind:      causes.CausePrimaryDelete,
		EventType: event,

		Namespace: namespace,
		Name:      name,
		UID:       uid,

		NewResourceVersion: resourceVersion,
		NewGeneration:      generation,

		ObservedAt: time.Now(),
	})

	return true
}

func (fpo ForPrimaryOpts) Update(e event.TypedUpdateEvent[client.Object]) bool {
	oldObj := e.ObjectOld
	newObj := e.ObjectNew

	generationChanged := oldObj.GetGeneration() != newObj.GetGeneration()

	cause := causes.CausePrimaryStatusOrMeta
	if generationChanged {
		cause = causes.CausePrimarySpecUpdate
	}

	watch := fpo.watchName
	event := causes.EventUpdate
	newObjNamespace := newObj.GetNamespace()
	newObjName := newObj.GetName()
	oldObjResourceVersion := oldObj.GetResourceVersion()
	newObjResourceVersion := newObj.GetResourceVersion()
	oldObjGeneration := oldObj.GetGeneration()
	newObjGeneration := newObj.GetGeneration()
	uid := newObj.GetUID()

	if fpo.a.printEventTrace {
		fmt.Printf(
			"[whyreconcile] watch=%s event=%s cause=%s namespace=%s name=%s resourceVersion=%s->%s generation=%d->%d generationChanged=%t\n",
			watch,
			event,
			cause,
			newObjNamespace,
			newObjName,
			oldObjResourceVersion,
			newObjResourceVersion,
			oldObjGeneration,
			newObjGeneration,
			generationChanged,
		)
	}

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: newObjNamespace,
			Name:      newObjName,
		},
	}

	fpo.a.store.Add(req, causes.Cause{
		WatchName: watch,
		Kind:      cause,
		EventType: event,

		Namespace: newObjNamespace,
		Name:      newObjName,
		UID:       uid,

		OldResourceVersion: oldObjResourceVersion,
		NewResourceVersion: newObjResourceVersion,

		OldGeneration: oldObjGeneration,
		NewGeneration: newObjGeneration,

		ObservedAt: time.Now(),
	})

	return true
}

func (fpo ForPrimaryOpts) Generic(e event.TypedGenericEvent[client.Object]) bool {
	watch := fpo.watchName
	event := causes.EventGeneric
	namespace := e.Object.GetNamespace()
	name := e.Object.GetName()
	resourceVersion := e.Object.GetResourceVersion()
	generation := e.Object.GetGeneration()
	uid := e.Object.GetUID()

	if fpo.a.printEventTrace {
		fmt.Printf(
			"[whyreconcile] watch=%s event=%s namespace=%s name=%s resourceVersion=%s generation=%d\n",
			watch,
			event,
			namespace,
			name,
			resourceVersion,
			generation,
		)
	}

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		},
	}

	fpo.a.store.Add(req, causes.Cause{
		WatchName: watch,
		Kind:      causes.CausePrimaryGeneric,
		EventType: event,

		Namespace: namespace,
		Name:      name,
		UID:       uid,

		NewResourceVersion: resourceVersion,
		NewGeneration:      generation,

		ObservedAt: time.Now(),
	})

	return true
}

func (a *Analyzer) NewForPrimaryOpts(watchName string, preds ...predicate.Predicate) builder.ForOption {
	p := ForPrimaryOpts{
		a:         a,
		watchName: watchName,
	}

	predicates := append(preds, p)

	return builder.WithPredicates(predicates...)
}

func (a *Analyzer) WrapReconcile(r reconcile.Reconciler) reconcile.Reconciler {
	wrapper := ReconcileWrapper{
		a:     a,
		inner: r,
	}

	return &wrapper
}

func (rc *ReconcileWrapper) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	causes := rc.a.store.Take(req)
	rc.a.printReconsileTrace(req, causes)

	return rc.inner.Reconcile(ctx, req)
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

	fmt.Printf(
		"[whyreconcile] controller=%s request=%s/%s causes=%d summary=%v\n",
		a.controllerName,
		req.Namespace,
		req.Name,
		len(eventCauses),
		formatCauseSummary(summary),
	)

	if !a.printEventTrace {
		return
	}

	for i, c := range eventCauses {
		fmt.Printf(
			"[whyreconcile] #%d watch=%s event=%s cause=%s namespace=%s name=%s resourceVersion=%s->%s generation=%d->%d observedAt=%s\n",
			i+1,
			c.WatchName,
			c.EventType,
			c.Kind,
			c.Namespace,
			c.Name,
			c.OldResourceVersion,
			c.NewResourceVersion,
			c.OldGeneration,
			c.NewGeneration,
			c.ObservedAt.Format(time.RFC3339Nano),
		)
	}
}

func formatCauseSummary(summary map[causes.CauseKind]int) string {
	parts := make([]string, 0, len(summary))

	for kind, count := range summary {
		parts = append(parts, fmt.Sprintf("%s:%d", kind, count))
	}

	sort.Strings(parts)

	return strings.Join(parts, ", ")
}

func NewAnalyzer(name string, s store.Store, printTrace bool) *Analyzer {
	if s == nil {
		s = store.NewCauseStore()
	}

	return &Analyzer{
		controllerName:  name,
		store:           s,
		printEventTrace: printTrace,
	}
}
