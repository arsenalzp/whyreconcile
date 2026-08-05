package analyzer

import (
	"time"

	"github.com/arsenalzp/whyreconcile/causes"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type OwnsSecondaryOpts struct {
	a         *Analyzer
	watchName string
}

func (oso OwnsSecondaryOpts) Create(e event.TypedCreateEvent[client.Object]) bool {
	watch := oso.watchName
	event := causes.EventCreate
	namespace := e.Object.GetNamespace()
	name := e.Object.GetName()
	resourceVersion := e.Object.GetResourceVersion()
	generation := e.Object.GetGeneration()
	uid := e.Object.GetUID()

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		},
	}

	cause := causes.Cause{
		WatchName: watch,
		Kind:      causes.CauseSecondaryCreate,
		EventType: event,

		Namespace: namespace,
		Name:      name,
		UID:       uid,

		NewResourceVersion: resourceVersion,
		NewGeneration:      generation,

		ObservedAt: time.Now(),
	}

	oso.a.store.Add(req, cause)

	if oso.a.printEventTrace {
		cause.PrintTraceCreate()
	}

	return true
}

func (oso OwnsSecondaryOpts) Delete(e event.TypedDeleteEvent[client.Object]) bool {
	watch := oso.watchName
	event := causes.EventDelete
	namespace := e.Object.GetNamespace()
	name := e.Object.GetName()
	resourceVersion := e.Object.GetResourceVersion()
	generation := e.Object.GetGeneration()
	uid := e.Object.GetUID()

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		},
	}

	cause := causes.Cause{
		WatchName: watch,
		Kind:      causes.CauseSecondaryDelete,
		EventType: event,

		Namespace: namespace,
		Name:      name,
		UID:       uid,

		NewResourceVersion: resourceVersion,
		NewGeneration:      generation,

		ObservedAt: time.Now(),
	}

	oso.a.store.Add(req, cause)

	if oso.a.printEventTrace {
		cause.PrintTraceDelete()
	}

	return true
}

func (oso OwnsSecondaryOpts) Update(e event.TypedUpdateEvent[client.Object]) bool {
	oldObj := e.ObjectOld
	newObj := e.ObjectNew

	generationChanged := oldObj.GetGeneration() != newObj.GetGeneration()

	cause := causes.CauseSecondaryStatusOrMeta
	if generationChanged {
		cause = causes.CauseSecondarySpecUpdate
	}

	watch := oso.watchName
	event := causes.EventUpdate
	newObjNamespace := newObj.GetNamespace()
	newObjName := newObj.GetName()
	oldObjResourceVersion := oldObj.GetResourceVersion()
	newObjResourceVersion := newObj.GetResourceVersion()
	oldObjGeneration := oldObj.GetGeneration()
	newObjGeneration := newObj.GetGeneration()
	uid := newObj.GetUID()

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

		Namespace: newObjNamespace,
		Name:      newObjName,
		UID:       uid,

		OldResourceVersion: oldObjResourceVersion,
		NewResourceVersion: newObjResourceVersion,

		OldGeneration: oldObjGeneration,
		NewGeneration: newObjGeneration,

		ObservedAt: time.Now(),
	}

	oso.a.store.Add(req, newCause)

	if oso.a.printEventTrace {
		newCause.PrintTraceUpdate()
	}

	return true
}

func (oso OwnsSecondaryOpts) Generic(e event.TypedGenericEvent[client.Object]) bool {
	watch := oso.watchName
	event := causes.EventGeneric
	namespace := e.Object.GetNamespace()
	name := e.Object.GetName()
	resourceVersion := e.Object.GetResourceVersion()
	generation := e.Object.GetGeneration()
	uid := e.Object.GetUID()

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		},
	}

	cause := causes.Cause{
		WatchName: watch,
		Kind:      causes.CauseSecondaryGeneric,
		EventType: event,

		Namespace: namespace,
		Name:      name,
		UID:       uid,

		NewResourceVersion: resourceVersion,
		NewGeneration:      generation,

		ObservedAt: time.Now(),
	}

	oso.a.store.Add(req, cause)

	if oso.a.printEventTrace {
		cause.PrintTraceGeneric()
	}

	return true
}
