package controllers

import (
	"context"
	"fmt"
	"reflect"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"

	porterv1 "get.porter.sh/operator/api/v1"
)

// AgentConfigReconciler reconciles an AgentConfig object
type AgentConfigReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=porter.sh,resources=credentialsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=porter.sh,resources=credentialsets/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=porter.sh,resources=credentialsets/finalizers,verbs=update

// SetupWithManager sets up the controller with the Manager.
func (r *AgentConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&porterv1.AgentConfig{}, builder.WithPredicates(resourceChanged{})).
		Owns(&porterv1.AgentAction{}).
		Complete(r)
}

// Reconcile is called when the spec of a credential set is changed
func (r *AgentConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("agentConfig", req.Name, "namespace", req.Namespace)

	ac := &porterv1.AgentConfig{}
	err := r.Get(ctx, req.NamespacedName, ac)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.V(Log5Trace).Info("Reconciliation skipped: CredentialSet CRD or one of its owned resources was deleted.")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	log = log.WithValues("resourceVersion", ac.ResourceVersion, "generation", ac.Generation)
	log.V(Log5Trace).Info("Reconciling credential set")

	// Check if we have requested an agent run yet
	action, handled, err := r.isHandled(ctx, log, ac)
	if err != nil {
		return ctrl.Result{}, err
	}

	if action != nil {
		log = log.WithValues("agentaction", action.Name)
	}

	if err = r.syncStatus(ctx, log, ac, action); err != nil {
		return ctrl.Result{}, err
	}

	if isDeleteProcessed(ac) {
		err = removeCredSetFinalizer(ctx, log, r.Client, ac)
		log.V(Log4Debug).Info("Reconciliation complete: Finalizer has been removed from the CredentialSet.")
		return ctrl.Result{}, err
	}

	if handled {
		// Check if retry was requested
		if action.GetRetryLabelValue() != ac.GetRetryLabelValue() {
			err = r.retry(ctx, log, ac, action)
			log.V(Log4Debug).Info("Reconciliation complete: The associated porter agent action was retried.")
			return ctrl.Result{}, err
		}

		//Nothing to do
		log.V(Log4Debug).Info("Reconciliation complete: A porter agent has already been dispatched.")
		return ctrl.Result{}, nil
	}

	if r.shouldDelete(ac) {
		err = r.runCredentialSet(ctx, log, ac)
		log.V(Log4Debug).Info("Reconciliation complete: A porter agent has been dispatched to delete the credential set")
		return ctrl.Result{}, err

	} else if isDeleted(ac) {
		log.V(Log4Debug).Info("Reconciliation complete: CredentialSet CRD is ready for deletion.")
		return ctrl.Result{}, nil
	}

	// ensure non-deleted credential sets have finalizers
	updated, err := ensureFinalizerSet(ctx, log, r.Client, ac)
	if err != nil {
		return ctrl.Result{}, err
	}
	if updated {
		// if we added a finalizer, stop processing and we will finish when the updated resource is reconciled
		log.V(Log4Debug).Info("Reconciliation complete: A finalizer has been set on the credential set.")
		return ctrl.Result{}, nil
	}
	err = r.runCredentialSet(ctx, log, ac)
	if err != nil {
		return ctrl.Result{}, err
	}
	log.V(Log4Debug).Info("Reconciliation complete: A porter agent has been dispatched to apply changes to the credential set.")
	return ctrl.Result{}, nil
}

// isHandled determines if this generation of the credential set resource has been processed by Porter
func (r *AgentConfigReconciler) isHandled(ctx context.Context, log logr.Logger, ac *porterv1.AgentConfig) (*porterv1.AgentAction, bool, error) {
	labels := getActionLabels(ac)
	results := porterv1.AgentActionList{}
	err := r.List(ctx, &results, client.InNamespace(ac.Namespace), client.MatchingLabels(labels))
	if err != nil {
		return nil, false, errors.Wrapf(err, "could not query for the current agent action")
	}

	if len(results.Items) == 0 {
		log.V(Log4Debug).Info("No existing agent action was found")
		return nil, false, nil
	}
	action := results.Items[0]
	log.V(Log4Debug).Info("Found existing agent action", "agentaction", action.Name, "namespace", action.Namespace)
	return &action, true, nil
}

// Check the status of the porter-agent job and use that to update the AgentAction status
func (r *AgentConfigReconciler) syncStatus(ctx context.Context, log logr.Logger, ac *porterv1.AgentConfig, action *porterv1.AgentAction) error {
	origStatus := ac.Status

	applyAgentAction(log, ac, action)

	if !reflect.DeepEqual(origStatus, ac.Status) {
		return r.saveStatus(ctx, log, ac)
	}

	return nil
}

// Only update the status with a PATCH, don't clobber the entire installation
func (r *AgentConfigReconciler) saveStatus(ctx context.Context, log logr.Logger, ac *porterv1.AgentConfig) error {
	log.V(Log5Trace).Info("Patching credential set status")
	return PatchObjectWithRetry(ctx, log, r.Client, r.Client.Status().Patch, ac, func() client.Object {
		return &porterv1.CredentialSet{}
	})
}

func (r *AgentConfigReconciler) shouldDelete(ac *porterv1.AgentConfig) bool {
	// ignore a deleted CRD with no finalizers
	return isDeleted(ac) && isFinalizerSet(ac)
}

func (r *AgentConfigReconciler) runCredentialSet(ctx context.Context, log logr.Logger, ac *porterv1.AgentConfig) error {
	log.V(Log5Trace).Info("Initializing credential set status")
	ac.Status.Initialize()
	if err := r.saveStatus(ctx, log, ac); err != nil {
		return err
	}

	return r.runPorter(ctx, log, ac)
}

// This could be the main "runFunction for each controller"
// Trigger an agent
func (r *AgentConfigReconciler) runPorter(ctx context.Context, log logr.Logger, ac *porterv1.AgentConfig) error {
	action, err := r.createAgentAction(ctx, log, ac)
	if err != nil {
		return err
	}

	// Update the CredentialSet Status with the agent action
	return r.syncStatus(ctx, log, ac, action)
}

// create a porter credentials AgentAction for applying or deleting credential sets
func (r *AgentConfigReconciler) createAgentAction(ctx context.Context, log logr.Logger, ac *porterv1.AgentConfig) (*porterv1.AgentAction, error) {
	credSetResourceB, err := ac.Spec.ToPorterDocument()
	if err != nil {
		return nil, err
	}

	action := newAgentAction(ac)
	log.WithValues("action name", action.Name)
	if r.shouldDelete(ac) {
		log.V(Log5Trace).Info("Deleting porter credential set")
		action.Spec.Args = []string{"credentials", "delete", "-n", ac.Spec.Namespace, ac.Spec.Name}
	} else {
		log.V(Log5Trace).Info(fmt.Sprintf("Creating porter credential set %s", action.Name))
		action.Spec.Args = []string{"credentials", "apply", "credentials.yaml"}
		action.Spec.Files = map[string][]byte{"credentials.yaml": credSetResourceB}
	}

	if err := r.Create(ctx, action); err != nil {
		return nil, errors.Wrap(err, "error creating the porter credential set agent action")
	}

	log.V(Log4Debug).Info("Created porter credential set agent action")
	return action, nil
}

// Sync the retry annotation from the credential set to the agent action to trigger another run.
func (r *AgentConfigReconciler) retry(ctx context.Context, log logr.Logger, ac *porterv1.AgentConfig, action *porterv1.AgentAction) error {
	log.V(Log5Trace).Info("Initializing installation status")
	ac.Status.Initialize()
	ac.Status.Action = &corev1.LocalObjectReference{Name: action.Name}
	if err := r.saveStatus(ctx, log, ac); err != nil {
		return err
	}

	log.V(Log5Trace).Info("Retrying associated porter agent action")
	retry := ac.GetRetryLabelValue()
	action.SetRetryAnnotation(retry)
	if err := r.Update(ctx, action); err != nil {
		return errors.Wrap(err, "error updating the associated porter agent action")
	}

	log.V(Log4Debug).Info("Retried associated porter agent action", "name", "retry", action.Name, retry)
	return nil
}
