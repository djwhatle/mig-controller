/*
Copyright 2019 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package migplan

import (
	"context"

	migapi "github.com/fusor/mig-controller/pkg/apis/migration/v1alpha1"
	"github.com/fusor/mig-controller/pkg/util"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	logf "sigs.k8s.io/controller-runtime/pkg/runtime/log"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller")

// Add creates a new MigPlan Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileMigPlan{Client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("migplan-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to MigPlan
	err = c.Watch(&source.Kind{
		Type: &migapi.MigPlan{}},
		&handler.EnqueueRequestForObject{},
		&UpdatedPredicate{},
	)
	if err != nil {
		return err
	}

	// Watch for changes to MigClusters referenced by MigPlans
	err = c.Watch(
		&source.Kind{Type: &migapi.MigCluster{}},
		&handler.EnqueueRequestsFromMapFunc{
			ToRequests: handler.ToRequestsFunc(MigClusterToMigPlan),
		})
	if err != nil {
		return err
	}

	// Watch for changes to MigStorage referenced by MigPlans
	err = c.Watch(
		&source.Kind{Type: &migapi.MigStorage{}},
		&handler.EnqueueRequestsFromMapFunc{
			ToRequests: handler.ToRequestsFunc(MigStorageToMigPlan),
		})
	if err != nil {
		return err
	}

	// Watch for changes to MigAssetCollections referenced by MigPlans
	err = c.Watch(
		&source.Kind{Type: &migapi.MigAssetCollection{}},
		&handler.EnqueueRequestsFromMapFunc{
			ToRequests: handler.ToRequestsFunc(MigAssetCollectionToMigPlan),
		})
	if err != nil {
		return err
	}

	return nil
}

var _ reconcile.Reconciler = &ReconcileMigPlan{}

// ReconcileMigPlan reconciles a MigPlan object
type ReconcileMigPlan struct {
	client.Client
	scheme *runtime.Scheme
}

// Automatically generate RBAC rules to allow the Controller to read and write Deployments
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=migration.openshift.io,resources=migplans,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=migration.openshift.io,resources=migplans/status,verbs=get;update;patch
func (r *ReconcileMigPlan) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	// Set up ResourceParentsMap to manage parent-child mapping
	rpm := util.GetResourceParentsMap()
	parentMigPlan := util.KubeResource{Kind: util.KindMigPlan, NsName: request.NamespacedName}

	// Fetch the MigPlan instance
	plan := &migapi.MigPlan{}
	err := r.Get(context.TODO(), request.NamespacedName, plan)
	if err != nil {
		if errors.IsNotFound(err) {
			// Object not found, return.  Created objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// Add all referenced
	childSrcCluster := util.KubeResource{
		Kind: util.KindMigCluster,
		NsName: types.NamespacedName{
			Name:      plan.Spec.SrcClusterRef.Name,
			Namespace: plan.Spec.SrcClusterRef.Namespace,
		},
	}
	rpm.AddChildToParent(childSrcCluster, parentMigPlan)

	childDestCluster := util.KubeResource{
		Kind: util.KindMigCluster,
		NsName: types.NamespacedName{
			Name:      plan.Spec.DestClusterRef.Name,
			Namespace: plan.Spec.DestClusterRef.Namespace,
		},
	}
	rpm.AddChildToParent(childDestCluster, parentMigPlan)

	childMigStorage := util.KubeResource{
		Kind: util.KindMigStorage,
		NsName: types.NamespacedName{
			Name:      plan.Spec.MigStorageRef.Name,
			Namespace: plan.Spec.MigStorageRef.Namespace,
		},
	}
	rpm.AddChildToParent(childMigStorage, parentMigPlan)

	childMigAssets := util.KubeResource{
		Kind: util.KindMigAssetCollection,
		NsName: types.NamespacedName{
			Name:      plan.Spec.MigAssetCollectionRef.Name,
			Namespace: plan.Spec.MigAssetCollectionRef.Namespace,
		},
	}
	rpm.AddChildToParent(childMigAssets, parentMigPlan)

	// Validations.
	// The 'nSet' is the number of conditions set during validation.
	err, nSet := r.validate(plan)
	if err != nil {
		return reconcile.Result{}, err
	}

	// Set the Ready condition
	if nSet == 0 {
		plan.Status.SetCondition(migapi.Condition{
			Type:    Ready,
			Status:  True,
			Message: ReadyMessage,
		})
	} else {
		plan.Status.DeleteCondition(Ready)
	}
	err = r.Update(context.TODO(), plan)
	if err != nil {
		return reconcile.Result{}, err
	}

	// Done
	return reconcile.Result{}, nil
}
