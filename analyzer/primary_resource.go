package analyzer

import (
	"time"

	"github.com/arsenalzp/whyreconcile/causes"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type ForPrimaryOpts struct {
	a         *Analyzer
	watchName string
}

func (fpo ForPrimaryOpts) Create(e event.TypedCreateEvent[client.Object]) bool {
	watch := fpo.watchName
	event := causes.EventCreate
	namespace := e.Object.GetNamespace()
	name := e.Object.GetName()
	resourceVersion := e.Object.GetResourceVersion()
	generation := e.Object.GetGeneration()

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		},
	}

	cause := causes.Cause{
		WatchName: watch,
		Kind:      causes.CausePrimaryCreate,
		EventType: event,

		Source: fpo.a.objectRef(e.Object),
		Target: causes.RequestRef{
			Namespace: namespace,
			Name:      name,
		},

		NewResourceVersion: resourceVersion,
		NewGeneration:      generation,

		ObservedAt: time.Now(),
	}

	fpo.a.store.Add(req, cause)

	if fpo.a.printEventTrace {
		cause.PrintTraceCreate()
	}

	return true
}

func (fpo ForPrimaryOpts) Delete(e event.TypedDeleteEvent[client.Object]) bool {
	watch := fpo.watchName
	event := causes.EventDelete
	namespace := e.Object.GetNamespace()
	name := e.Object.GetName()
	resourceVersion := e.Object.GetResourceVersion()
	generation := e.Object.GetGeneration()

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		},
	}

	cause := causes.Cause{
		WatchName: watch,
		Kind:      causes.CausePrimaryDelete,
		EventType: event,

		Source: fpo.a.objectRef(e.Object),
		Target: causes.RequestRef{
			Namespace: namespace,
			Name:      name,
		},

		NewResourceVersion: resourceVersion,
		NewGeneration:      generation,

		ObservedAt: time.Now(),
	}

	fpo.a.store.Add(req, cause)

	if fpo.a.printEventTrace {
		cause.PrintTraceDelete()
	}

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

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: newObjNamespace,
			Name:      newObjName,
		},
	}

	newCause := causes.Cause{
		WatchName: watch,
		Kind:      cause,
		EventType: event,

		Source: fpo.a.objectRef(newObj),
		Target: causes.RequestRef{
			Namespace: newObjNamespace,
			Name:      newObjName,
		},

		OldResourceVersion: oldObjResourceVersion,
		NewResourceVersion: newObjResourceVersion,

		OldGeneration: oldObjGeneration,
		NewGeneration: newObjGeneration,

		ObservedAt: time.Now(),
	}

	fpo.a.store.Add(req, newCause)

	if fpo.a.printEventTrace {
		newCause.PrintTraceUpdate()
	}

	return true
}

func (fpo ForPrimaryOpts) Generic(e event.TypedGenericEvent[client.Object]) bool {
	watch := fpo.watchName
	event := causes.EventGeneric
	namespace := e.Object.GetNamespace()
	name := e.Object.GetName()
	resourceVersion := e.Object.GetResourceVersion()
	generation := e.Object.GetGeneration()

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		},
	}

	cause := causes.Cause{
		WatchName: watch,
		Kind:      causes.CausePrimaryGeneric,
		EventType: event,

		Source: fpo.a.objectRef(e.Object),
		Target: causes.RequestRef{
			Namespace: namespace,
			Name:      name,
		},

		NewResourceVersion: resourceVersion,
		NewGeneration:      generation,

		ObservedAt: time.Now(),
	}

	fpo.a.store.Add(req, cause)

	if fpo.a.printEventTrace {
		cause.PrintTraceGeneric()
	}

	return true
}
