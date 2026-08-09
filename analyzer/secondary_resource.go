package analyzer

import (
	"context"
	"time"

	"github.com/arsenalzp/whyreconcile/causes"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type SecondaryResourceHandler struct {
	a         *Analyzer
	watchName string
	inner     handler.TypedEventHandler[client.Object, reconcile.Request]
}

func (hdlr SecondaryResourceHandler) Create(ctx context.Context, e event.TypedCreateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	watch := hdlr.watchName
	event := causes.EventCreate
	namespace := e.Object.GetNamespace()
	name := e.Object.GetName()
	resourceVersion := e.Object.GetResourceVersion()
	generation := e.Object.GetGeneration()

	cause := causes.Cause{
		WatchName: watch,
		Kind:      causes.CauseSecondaryCreate,
		EventType: event,

		Source: hdlr.a.objectRef(e.Object),
		Target: causes.RequestRef{
			Namespace: namespace,
			Name:      name,
		},

		NewResourceVersion: resourceVersion,
		NewGeneration:      generation,

		ObservedAt: time.Now(),
	}

	wrappedQueue := WrapQueue(q, hdlr.a.store, cause)

	hdlr.inner.Create(ctx, e, wrappedQueue)
}

func (hdlr SecondaryResourceHandler) Delete(ctx context.Context, e event.TypedDeleteEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	watch := hdlr.watchName
	event := causes.EventDelete
	namespace := e.Object.GetNamespace()
	name := e.Object.GetName()
	resourceVersion := e.Object.GetResourceVersion()
	generation := e.Object.GetGeneration()

	cause := causes.Cause{
		WatchName: watch,
		Kind:      causes.CauseSecondaryDelete,
		EventType: event,

		Source: hdlr.a.objectRef(e.Object),
		Target: causes.RequestRef{
			Namespace: namespace,
			Name:      name,
		},

		NewResourceVersion: resourceVersion,
		NewGeneration:      generation,

		ObservedAt: time.Now(),
	}

	wrappedQueue := WrapQueue(q, hdlr.a.store, cause)

	hdlr.inner.Delete(ctx, e, wrappedQueue)

}

func (hdlr SecondaryResourceHandler) Update(ctx context.Context, e event.TypedUpdateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	oldObj := e.ObjectOld
	newObj := e.ObjectNew

	generationChanged := oldObj.GetGeneration() != newObj.GetGeneration()

	cause := causes.CauseSecondaryStatusOrMeta
	if generationChanged {
		cause = causes.CauseSecondarySpecUpdate
	}

	watch := hdlr.watchName
	event := causes.EventUpdate
	newObjNamespace := newObj.GetNamespace()
	newObjName := newObj.GetName()
	oldObjResourceVersion := oldObj.GetResourceVersion()
	newObjResourceVersion := newObj.GetResourceVersion()
	oldObjGeneration := oldObj.GetGeneration()
	newObjGeneration := newObj.GetGeneration()

	newCause := causes.Cause{
		WatchName: watch,
		Kind:      cause,
		EventType: event,

		Source: hdlr.a.objectRef(newObj),
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

	wrappedQueue := WrapQueue(q, hdlr.a.store, newCause)

	hdlr.inner.Update(ctx, e, wrappedQueue)
}

func (hdlr SecondaryResourceHandler) Generic(ctx context.Context, e event.TypedGenericEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	watch := hdlr.watchName
	event := causes.EventGeneric
	namespace := e.Object.GetNamespace()
	name := e.Object.GetName()
	resourceVersion := e.Object.GetResourceVersion()
	generation := e.Object.GetGeneration()

	cause := causes.Cause{
		WatchName: watch,
		Kind:      causes.CauseSecondaryGeneric,
		EventType: event,

		Source: hdlr.a.objectRef(e.Object),
		Target: causes.RequestRef{
			Namespace: namespace,
			Name:      name,
		},

		NewResourceVersion: resourceVersion,
		NewGeneration:      generation,

		ObservedAt: time.Now(),
	}

	wrappedQueue := WrapQueue(q, hdlr.a.store, cause)

	hdlr.inner.Generic(ctx, e, wrappedQueue)
}
