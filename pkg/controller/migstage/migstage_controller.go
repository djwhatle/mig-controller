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

package migstage

import (
	"context"
	"fmt"
	"reflect"

	migapi "github.com/fusor/mig-controller/pkg/apis/migration/v1alpha1"
	"github.com/fusor/mig-controller/pkg/controller/migassetcollection"
	"github.com/fusor/mig-controller/pkg/controller/migplan"
	"github.com/fusor/mig-controller/pkg/util"
	velerov1 "github.com/heptio/velero/pkg/apis/velero/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clusterregv1alpha1 "k8s.io/cluster-registry/pkg/apis/clusterregistry/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	logf "sigs.k8s.io/controller-runtime/pkg/runtime/log"
	"sigs.k8s.io/controller-runtime/pkg/source"

	kapi "k8s.io/api/core/v1"
)

var log = logf.Log.WithName("controller")

// Add creates a new MigStage Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileMigStage{Client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("migstage-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to MigStage
	err = c.Watch(&source.Kind{Type: &migapi.MigStage{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to MigPlans referenced by MigStages
	err = c.Watch(
		&source.Kind{Type: &migapi.MigPlan{}},
		&handler.EnqueueRequestsFromMapFunc{
			ToRequests: handler.ToRequestsFunc(MigPlanToMigStage),
		})
	if err != nil {
		return err
	}

	// Watch for changes to MigClusters referenced by MigStages
	err = c.Watch(
		&source.Kind{Type: &migapi.MigCluster{}},
		&handler.EnqueueRequestsFromMapFunc{
			ToRequests: handler.ToRequestsFunc(MigClusterToMigStage),
		})
	if err != nil {
		return err
	}

	return nil
}

var _ reconcile.Reconciler = &ReconcileMigStage{}

// ReconcileMigStage reconciles a MigStage object
type ReconcileMigStage struct {
	client.Client
	scheme *runtime.Scheme
}

// Reconcile reads that state of the cluster for a MigStage object and makes changes based on the state read
// and what is in the MigStage.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  The scaffolding writes
// a Deployment as an example
// Automatically generate RBAC rules to allow the Controller to read and write Deployments
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=migration.openshift.io,resources=migstages,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=migration.openshift.io,resources=migstages/status,verbs=get;update;patch
func (r *ReconcileMigStage) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	// TODO: instead of individually getting each piece of the data model, see if it's
	// possible to write a function that will compile one struct with all of the info
	// we need to perform a stage operation in one place. Would make this reconcile function
	// much cleaner.

	// Hardcode Velero namespace for now
	veleroNs := "velero"

	// Set up ResourceParentsMap to manage parent-child mapping
	rpm := util.GetResourceParentsMap()
	parentMigStage := util.KubeResource{Kind: util.KindMigStage, NsName: request.NamespacedName}

	// Fetch the MigStage instance
	instance := &migapi.MigStage{}
	err := r.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil // don't requeue
		}
		return reconcile.Result{}, err // requeue
	}

	// Retrieve MigPlan from ref
	migPlanRef := instance.Spec.MigPlanRef
	migPlan := &migapi.MigPlan{}
	err = r.Get(context.TODO(), types.NamespacedName{Name: migPlanRef.Name, Namespace: migPlanRef.Namespace}, migPlan)
	if err != nil {
		log.Info(fmt.Sprintf("[mStage] Error getting MigPlan [%s/%s]", migPlanRef.Namespace, migPlanRef.Name))
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil // don't requeue
		}
		return reconcile.Result{}, err // requeue
	}

	// Valid MigPlan found, add MigStage as parent to receive reconciliation events
	childMigPlan := util.KubeResource{
		Kind: util.KindMigPlan,
		NsName: types.NamespacedName{
			Name:      migPlanRef.Name,
			Namespace: migPlanRef.Namespace,
		},
	}
	rpm.AddChildToParent(childMigPlan, parentMigStage)

	// Check for 'Ready' condition on referenced MigPlan
	_, migPlanReady := migPlan.Status.FindCondition(migplan.Ready)
	if migPlanReady == nil {
		log.Info(fmt.Sprintf("[mStage] Referenced MigPlan [%s/%s] was not ready.", migPlanRef.Namespace, migPlanRef.Name))
		return reconcile.Result{}, nil // don't requeue
	}

	// [TODO] Create Velero BackupStorageLocation if needed

	// [TODO] Create Velero VolumeSnapshotLocation if needed

	// Retrieve MigAssetCollection from ref
	assetsRef := migPlan.Spec.MigAssetCollectionRef
	assets := &migapi.MigAssetCollection{}
	err = r.Get(context.TODO(), types.NamespacedName{Name: assetsRef.Name, Namespace: assetsRef.Namespace}, assets)
	if err != nil {
		log.Info(fmt.Sprintf("[mStage] Error getting MigAssetCollection [%s/%s]", assetsRef.Namespace, assetsRef.Name))
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil // don't requeue
		}
		return reconcile.Result{}, err // requeue
	}

	// Check for 'Ready' condition on referenced MigAssetCollection
	_, assetsReady := assets.Status.FindCondition(migassetcollection.Ready)
	if assetsReady == nil {
		log.Info(fmt.Sprintf("[mStage] Referenced MigAssetCollection [%s/%s] was not ready.", assetsRef.Namespace, assetsRef.Name))
		return reconcile.Result{}, nil // don't requeue
	}

	// ###########################################
	// ###########################################
	// ###########################################
	// ###########################################
	// Get the srcCluster MigCluster
	srcClusterRef := migPlan.Spec.SrcClusterRef
	srcCluster := &migapi.MigCluster{}
	err = r.Get(context.TODO(), types.NamespacedName{Name: srcClusterRef.Name, Namespace: srcClusterRef.Namespace}, srcCluster)
	if err != nil {
		log.Info(fmt.Sprintf("[mStage] Error getting srcCluster MigCluster [%s/%s]", srcClusterRef.Namespace, srcClusterRef.Name))
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil // don't requeue
		}
		return reconcile.Result{}, err // requeue
	}

	// TODO: dedupe this code section (shared with MigCluster controller)
	// Get the cluster-registry Cluster from MigCluster
	srcCrClusterRef := srcCluster.Spec.ClusterRef
	srcCrCluster := &clusterregv1alpha1.Cluster{}
	err = r.Get(context.TODO(), types.NamespacedName{Name: srcCrClusterRef.Name, Namespace: srcCrClusterRef.Namespace}, srcCrCluster)
	if err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil // don't requeue
		}
		return reconcile.Result{}, err // requeue
	}

	var srcRemoteClusterURL string
	srcK8sEndpoints := srcCrCluster.Spec.KubernetesAPIEndpoints.ServerEndpoints
	if len(srcK8sEndpoints) > 0 {
		srcRemoteClusterURL = string(srcK8sEndpoints[0].ServerAddress)
		log.Info(fmt.Sprintf("[mStage] srcRemoteClusterURL: [%s]", srcRemoteClusterURL))
	} else {
		log.Info(fmt.Sprintf("[mStage] srcRemoteClusterURL: [len=0]"))
	}

	// TODO: dedupe this code section (shared with MigCluster controller)
	// Get SA token from srcCluster
	srcSaSecretRef := srcCluster.Spec.ServiceAccountSecretRef
	srcSaSecret := &kapi.Secret{}
	err = r.Get(context.TODO(), types.NamespacedName{Name: srcSaSecretRef.Name, Namespace: srcSaSecretRef.Namespace}, srcSaSecret)
	if err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil // don't requeue
		}
		return reconcile.Result{}, err // requeue
	}
	// Get data from srcSaToken secret
	srcSaTokenKey := "saToken"
	srcSaTokenData, ok := srcSaSecret.Data[srcSaTokenKey]
	if !ok {
		log.Info(fmt.Sprintf("[mCluster] srcSaToken: [%v]", ok))
		return reconcile.Result{}, nil // don't requeue
	}
	srcSaToken := string(srcSaTokenData)

	// Create srcCluster client which we will use to create a Velero Backup
	srcClusterRestConfig := util.BuildRestConfig(srcRemoteClusterURL, srcSaToken)
	srcClusterK8sClient, err := util.BuildControllerRuntimeClient(srcClusterRestConfig)
	if err != nil {
		log.Error(err, "Failed to GET srcClusterK8sClient")
		return reconcile.Result{}, nil
	}
	// ###########################################
	// ###########################################
	// ###########################################
	// ###########################################

	// Create Velero Backup on srcCluster looking at namespaces in MigAssetCollection referenced by MigPlan
	backupNamespaces := assets.Spec.Namespaces
	backupUniqueName := fmt.Sprintf("%s-velero-backup", instance.Name)
	vBackupNew := util.BuildVeleroBackup(veleroNs, backupUniqueName, backupNamespaces)

	vBackupExisting := &velerov1.Backup{}
	err = srcClusterK8sClient.Get(context.TODO(), types.NamespacedName{Name: vBackupNew.Name, Namespace: vBackupNew.Namespace}, vBackupExisting)
	if err != nil {
		if errors.IsNotFound(err) {
			// Backup not found
			err = srcClusterK8sClient.Create(context.TODO(), vBackupNew)
			if err != nil {
				log.Error(err, "[mStage] Exit: Failed to CREATE Velero Backup")
				return reconcile.Result{}, nil
			}
			log.Info("[mStage] Velero Backup CREATED successfully")
		}
		// Error reading the 'Backup' object - requeue the request.
		log.Error(err, "[mStage] Exit 4: Requeueing")
		return reconcile.Result{}, err
	}

	if !reflect.DeepEqual(vBackupNew.Spec, vBackupExisting.Spec) {
		// Send "Create" action for Velero Backup to K8s API
		vBackupExisting.Spec = vBackupNew.Spec
		err = srcClusterK8sClient.Update(context.TODO(), vBackupExisting)
		if err != nil {
			log.Error(err, "[mStage] Failed to UPDATE Velero Backup")
			return reconcile.Result{}, nil
		}
		log.Info("[mStage] Velero Backup UPDATED successfully")
	} else {
		log.Info("[mStage] Velero Backup EXISTS already")
	}

	// Monitor changes to Velero Backup state on srcCluster, wait for completion (watch MigCluster)

	// Create Velero Restore on dstCluster pointing at Velero Backup unique name

	// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
	// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
	// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
	// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
	// Get the destCluster MigCluster
	destClusterRef := migPlan.Spec.DestClusterRef
	destCluster := &migapi.MigCluster{}
	err = r.Get(context.TODO(), types.NamespacedName{Name: destClusterRef.Name, Namespace: destClusterRef.Namespace}, destCluster)
	if err != nil {
		log.Info(fmt.Sprintf("[mStage] Error getting destCluster MigCluster [%s/%s]", destClusterRef.Name, destClusterRef.Namespace))
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil // don't requeue
		}
		return reconcile.Result{}, err // requeue
	}

	// TODO: dedupe this code section (shared with MigCluster controller)
	// Get the cluster-registry Cluster from MigCluster
	destCrClusterRef := destCluster.Spec.ClusterRef
	destCrCluster := &clusterregv1alpha1.Cluster{}
	err = r.Get(context.TODO(), types.NamespacedName{Name: destCrClusterRef.Name, Namespace: destCrClusterRef.Namespace}, destCrCluster)
	if err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil // don't requeue
		}
		return reconcile.Result{}, err // requeue
	}

	var destRemoteClusterURL string
	destK8sEndpoints := destCrCluster.Spec.KubernetesAPIEndpoints.ServerEndpoints
	if len(destK8sEndpoints) > 0 {
		destRemoteClusterURL = string(destK8sEndpoints[0].ServerAddress)
		log.Info(fmt.Sprintf("[mStage] destRemoteClusterURL: [%s]", destRemoteClusterURL))
	} else {
		log.Info(fmt.Sprintf("[mStage] destRemoteClusterURL: [len=0]"))
	}

	// TODO: dedupe this code section (shared with MigCluster controller)
	// Get SA token from destCluster
	destSaSecretRef := destCluster.Spec.ServiceAccountSecretRef
	destSaSecret := &kapi.Secret{}
	err = r.Get(context.TODO(), types.NamespacedName{Name: destSaSecretRef.Name, Namespace: destSaSecretRef.Namespace}, destSaSecret)
	if err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil // don't requeue
		}
		return reconcile.Result{}, err // requeue
	}
	// Get data from destSaToken secret
	destSaTokenKey := "saToken"
	destSaTokenData, ok := destSaSecret.Data[destSaTokenKey]
	if !ok {
		log.Info(fmt.Sprintf("[mCluster] destSaToken: [%v]", ok))
		return reconcile.Result{}, nil // don't requeue
	}
	destSaToken := string(destSaTokenData)

	// Create destCluster client which we will use to create a Velero Backup
	destClusterRestConfig := util.BuildRestConfig(destRemoteClusterURL, destSaToken)
	destClusterK8sClient, err := util.BuildControllerRuntimeClient(destClusterRestConfig)
	if err != nil {
		log.Error(err, "Failed to GET destClusterK8sClient")
		return reconcile.Result{}, nil
	}
	// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
	// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
	// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
	// &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

	restoreUniqueName := fmt.Sprintf("%s-velero-restore", instance.Name)
	vRestoreNew := util.BuildVeleroRestore(veleroNs, restoreUniqueName, backupUniqueName)

	vRestoreExisting := &velerov1.Restore{}
	err = destClusterK8sClient.Get(context.TODO(), types.NamespacedName{Name: vRestoreNew.Name, Namespace: vRestoreNew.Namespace}, vRestoreExisting)
	if err != nil {
		if errors.IsNotFound(err) {
			// Backup not found
			err = destClusterK8sClient.Create(context.TODO(), vRestoreNew)
			if err != nil {
				log.Error(err, "[mStage] Exit: Failed to CREATE Velero Restore")
				return reconcile.Result{}, nil
			}
			log.Info("[mStage] Velero Restore CREATED successfully")
		}
		// Error reading the 'Backup' object - requeue the request.
		log.Error(err, "[mStage] Exit 4: Requeueing")
		return reconcile.Result{}, err
	}

	if !reflect.DeepEqual(vRestoreNew.Spec, vRestoreExisting.Spec) {
		// Send "Create" action for Velero Backup to K8s API
		vRestoreExisting.Spec = vRestoreNew.Spec
		err = destClusterK8sClient.Update(context.TODO(), vRestoreExisting)
		if err != nil {
			log.Error(err, "[mStage] Failed to UPDATE Velero Restore")
			return reconcile.Result{}, nil
		}
		log.Info("[mStage] Velero Restore UPDATED successfully")
	} else {
		log.Info("[mStage] Velero Restore EXISTS already")
	}

	// Monitor changes to Velero Restore state on dstCluster, wait for completion (watch MigCluster)

	// Mark MigStage as complete

	return reconcile.Result{}, nil
}
