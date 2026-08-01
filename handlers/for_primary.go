package handlers

import (
	"context"
	"fmt"
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
	_ = rc.a.store.Take(req)
	return rc.inner.Reconcile(ctx, req)
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
