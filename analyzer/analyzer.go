package analyzer

import (
	"context"
	"fmt"
	"time"

	"github.com/arsenalzp/whyreconcile/causes"
	"github.com/arsenalzp/whyreconcile/store"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type Analyzer struct {
	controllerName  string
	store           store.Store
	printEventTrace bool
}

type ReconcileWrapper struct {
	a     *Analyzer
	inner reconcile.Reconciler
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

func (a *Analyzer) NewForPrimaryOpts(watchName string, preds ...predicate.Predicate) builder.ForOption {
	p := ForPrimaryOpts{
		a:         a,
		watchName: watchName,
	}

	predicates := append(preds, p)

	return builder.WithPredicates(predicates...)
}

func (a *Analyzer) NewOwnsSecondaryOpts(watchName string, preds ...predicate.Predicate) builder.ForOption {
	p := OwnsSecondaryOpts{
		a:         a,
		watchName: watchName,
	}

	predicates := append(preds, p)

	return builder.WithPredicates(predicates...)
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
