//go:build integration

package integration_test

import (
	"context"
	"time"

	porterv1 "get.porter.sh/operator/api/v1"
	"github.com/carolynvs/magex/shx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type porterResource interface {
	client.Object
	GetStatus() porterv1.PorterResourceStatus
}

func waitForPorter(ctx context.Context, resource porterResource, namespace, name, msg string) error {
	Log("%s: %s/%s", msg, namespace, name)
	key := client.ObjectKey{Namespace: namespace, Name: name}
	ctx, cancel := context.WithTimeout(ctx, getWaitTimeout())
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			Fail(errors.Wrapf(ctx.Err(), "timeout %s", msg).Error())
		default:
			err := k8sClient.Get(ctx, key, resource)
			if err != nil {
				// There is lag between creating and being able to retrieve, I don't understand why
				if apierrors.IsNotFound(err) {
					time.Sleep(time.Second)
					continue
				}
				return err
			}
			status := resource.GetStatus()
			// Check if the latest change has been processed
			if resource.GetGeneration() == status.ObservedGeneration {
				if apimeta.IsStatusConditionTrue(status.Conditions, string(porterv1.ConditionComplete)) {
					return nil
				}

				if apimeta.IsStatusConditionTrue(status.Conditions, string(porterv1.ConditionFailed)) {
					// Grab some extra info to help with debugging
					debugFailedResource(ctx, status.Action.Name, namespace)
					return errors.New("porter did not run successfully")
				}
			}

			time.Sleep(time.Second)
			continue
		}
	}
}

func waitForResourceDeleted(ctx context.Context, resource porterResource, namespace, name string) error {
	Log("Waiting for resource to finish deleting: %s/%s", namespace, name)
	key := client.ObjectKey{Namespace: namespace, Name: name}
	waitCtx, cancel := context.WithTimeout(ctx, getWaitTimeout())
	defer cancel()
	for {
		select {
		case <-waitCtx.Done():
			Fail(errors.Wrap(waitCtx.Err(), "timeout waiting for CredentialSet to delete").Error())
		default:
			err := k8sClient.Get(ctx, key, resource)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return nil
				}
				return err
			}

			time.Sleep(time.Second)
			continue
		}
	}
}

func debugFailedResource(ctx context.Context, namespace, name string) {

	Log("DEBUG: ----------------------------------------------------")
	actionKey := client.ObjectKey{Name: name, Namespace: namespace}
	action := &porterv1.AgentAction{}
	if err := k8sClient.Get(ctx, actionKey, action); err != nil {
		Log(errors.Wrap(err, "could not retrieve the Installation's AgentAction to troubleshoot").Error())
		return
	}

	jobKey := client.ObjectKey{Name: action.Status.Job.Name, Namespace: action.Namespace}
	job := &batchv1.Job{}
	if err := k8sClient.Get(ctx, jobKey, job); err != nil {
		Log(errors.Wrap(err, "could not retrieve the Installation's Job to troubleshoot").Error())
		return
	}

	shx.Command("kubectl", "logs", "-n="+job.Namespace, "job/"+job.Name).
		Env("KUBECONFIG=" + "../../kind.config").RunV()
	Log("DEBUG: ----------------------------------------------------")
}

func validateResourceConditions(resource porterResource) {
	status := resource.GetStatus()
	// Checks that all expected conditions are set
	Expect(apimeta.IsStatusConditionTrue(status.Conditions, string(porterv1.ConditionScheduled)))
	Expect(apimeta.IsStatusConditionTrue(status.Conditions, string(porterv1.ConditionStarted)))
	Expect(apimeta.IsStatusConditionTrue(status.Conditions, string(porterv1.ConditionComplete)))
}
