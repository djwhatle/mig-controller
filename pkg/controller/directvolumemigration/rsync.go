package directvolumemigration

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/rsa"
	"encoding/hex"
	"fmt"
	random "math/rand"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"

	migapi "github.com/konveyor/mig-controller/pkg/apis/migration/v1alpha1"
	"github.com/konveyor/mig-controller/pkg/compat"
	migsettings "github.com/konveyor/mig-controller/pkg/settings"
	routev1 "github.com/openshift/api/route/v1"
	"golang.org/x/crypto/ssh"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type pvc struct {
	Name string
}

type rsyncConfig struct {
	SshUser   string
	Namespace string
	Password  string
	PVCList   []pvc
}

const (
	TRANSFER_POD_CPU_LIMIT      = "TRANSFER_POD_CPU_LIMIT"
	TRANSFER_POD_MEMORY_LIMIT   = "TRANSFER_POD_MEMORY_LIMIT"
	TRANSFER_POD_CPU_REQUEST    = "TRANSFER_POD_CPU_REQUEST"
	TRANSFER_POD_MEMORY_REQUEST = "TRANSFER_POD_MEMORY_REQUEST"
	CLIENT_POD_CPU_LIMIT        = "CLIENT_POD_CPU_LIMIT"
	CLIENT_POD_MEMORY_LIMIT     = "CLIENT_POD_MEMORY_LIMIT"
	CLIENT_POD_CPU_REQUEST      = "CLIENT_POD_CPU_REQUEST"
	CLIENT_POD_MEMORY_REQUEST   = "CLIENT_POD_MEMORY_REQUEST"
	STUNNEL_POD_CPU_LIMIT       = "STUNNEL_POD_CPU_LIMIT"
	STUNNEL_POD_MEMORY_LIMIT    = "STUNNEL_POD_MEMORY_LIMIT"
	STUNNEL_POD_CPU_REQUEST     = "STUNNEL_POD_CPU_REQUEST"
	STUNNEL_POD_MEMORY_REQUEST  = "STUNNEL_POD_MEMORY_REQUEST"

	// DefaultStunnelTimout is when stunnel timesout on establishing connection from source to destination.
	//  When this timeout is reached, the rsync client will still see "connection reset by peer". It is a red-herring
	// it does not conclusively mean the destination rsyncd is unhealthy but stunnel is dropping this in between
	DefaultStunnelTimeout = 20
)

// TODO: Parameterize this more to support custom
// user/pass/networking configs from directvolumemigration spec
const rsyncConfigTemplate = `apiVersion: v1
kind: ConfigMap
metadata:
  labels:
    purpose: rsync
data:
  rsyncd.conf: |
    syslog facility = local7
    read only = no
    list = yes
    log file = /dev/stdout
    max verbosity = 4
    auth users = {{ .SshUser }}
    secrets file = /etc/rsyncd.secrets
    hosts allow = ::1, 127.0.0.1, localhost
    uid = root
    gid = root
    {{ range $i, $pvc := .PVCList }}
    [{{ $pvc.Name }}]
        comment = archive for {{ $pvc.Name }}
        path = /mnt/{{ $.Namespace }}/{{ $pvc.Name }}
        use chroot = no
        munge symlinks = no
        list = yes
        hosts allow = ::1, 127.0.0.1, localhost
        auth users = {{ $.SshUser }}
        secrets file = /etc/rsyncd.secrets
        read only = false
   {{ end }}
`

func (t *Task) areRsyncTransferPodsRunning() (bool, error) {
	// Get client for destination
	destClient, err := t.getDestinationClient()
	if err != nil {
		return false, err
	}

	pvcMap := t.getPVCNamespaceMap()
	dvmLabels := t.buildDVMLabels()
	dvmLabels["purpose"] = DirectVolumeMigrationRsync
	selector := labels.SelectorFromSet(dvmLabels)

	for ns, _ := range pvcMap {
		pods := corev1.PodList{}
		err = destClient.List(
			context.TODO(),
			&k8sclient.ListOptions{
				Namespace:     ns,
				LabelSelector: selector,
			},
			&pods)
		if err != nil {
			return false, err
		}
		if len(pods.Items) != 1 {
			t.Log.Info(fmt.Sprintf("dvm cr: %s/%s, number of rsync pods expected %d, found %d", t.Owner.Namespace, t.Owner.Name, 1, len(pods.Items)))
			return false, nil
		}
		for _, pod := range pods.Items {
			if pod.Status.Phase != corev1.PodRunning {
				for _, podCond := range pod.Status.Conditions {
					if podCond.Reason == corev1.PodReasonUnschedulable {
						t.Log.Info(fmt.Sprintf("Found UNSCHEDULABLE Rsync Transfer Pod [%v/%v] "+
							"with Phase=[%v] on destination cluster. Message: [%v].",
							pod.Namespace, pod.Name, pod.Status.Phase, podCond.Message))
						return false, nil
					}
				}
				t.Log.Info(fmt.Sprintf("Found non-running Rsync Transfer Pod [%v/%v] "+
					"with Phase=[%v] on destination cluster.",
					pod.Namespace, pod.Name, pod.Status.Phase))
				return false, nil
			}
		}
	}

	return true, nil

	// Create rsync transfer pod on destination

	// Create rsync client pod on source
}

// Generate SSH keys to be used
// TODO: Need to determine if this has already been generated and
// not to regenerate
func (t *Task) generateSSHKeys() error {
	// Check if already generated
	if t.SSHKeys != nil {
		return nil
	}
	// Private Key generation
	privateKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return err
	}

	// Validate Private Key
	err = privateKey.Validate()
	if err != nil {
		return err
	}

	t.SSHKeys = &sshKeys{
		PublicKey:  &privateKey.PublicKey,
		PrivateKey: privateKey,
	}
	return nil
}

func (t *Task) createRsyncConfig() error {
	// Get client for destination
	destClient, err := t.getDestinationClient()
	if err != nil {
		return err
	}
	// Get client for source
	srcClient, err := t.getSourceClient()
	if err != nil {
		return err
	}

	password, err := t.getRsyncPassword()
	if err != nil {
		return err
	}
	if password == "" {
		password, err = t.createRsyncPassword()
		if err != nil {
			return err
		}
	}

	// Create rsync configmap/secret on source + destination
	// Create rsync secret (which contains user/pass for rsync transfer pod) in
	// each namespace being migrated
	// Needs to go in every namespace where a PVC is being migrated
	pvcMap := t.getPVCNamespaceMap()

	for ns, vols := range pvcMap {
		pvcList := []pvc{}
		for _, vol := range vols {
			pvcList = append(pvcList, pvc{Name: vol.Name})
		}
		// Generate template
		rsyncConf := rsyncConfig{
			SshUser:   "root",
			Namespace: ns,
			PVCList:   pvcList,
			Password:  password,
		}
		var tpl bytes.Buffer
		temp, err := template.New("config").Parse(rsyncConfigTemplate)
		if err != nil {
			return err
		}
		err = temp.Execute(&tpl, rsyncConf)
		if err != nil {
			return err
		}

		configMap := corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      DirectVolumeMigrationRsyncConfig,
				Labels: map[string]string{
					"app": DirectVolumeMigrationRsyncTransfer,
				},
			},
		}
		err = yaml.Unmarshal(tpl.Bytes(), &configMap)
		if err != nil {
			return err
		}

		// Create configmap on source + dest
		// Note: when this configmap changes the rsync pod
		// needs to restart
		// Need to launch new pod when configmap changes
		t.Log.Info(fmt.Sprintf("Creating Rsync Transfer Pod ConfigMap [%v/%v] on destination cluster",
			configMap.Namespace, configMap.Name))
		err = destClient.Create(context.TODO(), &configMap)
		if k8serror.IsAlreadyExists(err) {
			t.Log.Info("Configmap already exists on destination", "namespace", configMap.Namespace)
		} else if err != nil {
			return err
		}

		// Before starting rsync transfer pod, must generate rsync password in a
		// secret and pass it into the transfer pod

		// Format user:password
		// Put this string into /etc/rsyncd.secrets in rsync transfer pod
		// Rsyncd configmap references this file as "secrets file":
		// https://github.com/konveyor/pvc-migrate/blob/master/3_run_rsync/templates/rsyncd.yml.j2#L17
		// This configmap also takes in the user name as an "auth user". (root)
		// Make this user configurable on CR spec?

		// For source side, create secret with user/password and
		// mount as environment variables into rsync client pod
		srcSecret := corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      DirectVolumeMigrationRsyncCreds,
				Labels: map[string]string{
					"app": DirectVolumeMigrationRsyncTransfer,
				},
			},
			Data: map[string][]byte{
				"RSYNC_PASSWORD": []byte(password),
			},
		}
		t.Log.Info(fmt.Sprintf("Creating Rsync Password Secret [%v/%v] on source cluster",
			srcSecret.Namespace, srcSecret.Name))
		err = srcClient.Create(context.TODO(), &srcSecret)
		if k8serror.IsAlreadyExists(err) {
			t.Log.Info("Secret already exists on source", "namespace", srcSecret.Namespace)
		} else if err != nil {
			return err
		}
		destSecret := corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      DirectVolumeMigrationRsyncCreds,
				Labels: map[string]string{
					"app": DirectVolumeMigrationRsyncTransfer,
				},
			},
			Data: map[string][]byte{
				"credentials": []byte("root:" + password),
			},
		}
		t.Log.Info(fmt.Sprintf("Creating Rsync Password Secret [%v/%v] on destination cluster",
			destSecret.Namespace, destSecret.Name))
		err = destClient.Create(context.TODO(), &destSecret)
		if k8serror.IsAlreadyExists(err) {
			t.Log.Info("Secret already exists on destination", "namespace", destSecret.Namespace)
		} else if err != nil {
			return err
		}
	}

	// One rsync transfer pod per namespace
	// One rsync client pod per PVC

	// Also in this rsyncd configmap, include all PVC mount paths, see:
	// https://github.com/konveyor/pvc-migrate/blob/master/3_run_rsync/templates/rsyncd.yml.j2#L23

	return nil
}

// Create rsync transfer route
func (t *Task) createRsyncTransferRoute() error {
	// Get client for destination
	destClient, err := t.getDestinationClient()
	if err != nil {
		return err
	}
	pvcMap := t.getPVCNamespaceMap()
	dvmLabels := t.buildDVMLabels()
	dvmLabels["purpose"] = DirectVolumeMigrationRsync

	for ns, _ := range pvcMap {
		svc := corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      DirectVolumeMigrationRsyncTransferSvc,
				Namespace: ns,
				Labels: map[string]string{
					"app": DirectVolumeMigrationRsyncTransfer,
				},
			},
			Spec: corev1.ServiceSpec{
				Ports: []corev1.ServicePort{
					{
						Name:       DirectVolumeMigrationStunnel,
						Protocol:   corev1.ProtocolTCP,
						Port:       int32(2222),
						TargetPort: intstr.IntOrString{Type: intstr.Int, IntVal: 2222},
					},
				},
				Selector: dvmLabels,
				Type:     corev1.ServiceTypeClusterIP,
			},
		}
		t.Log.Info(fmt.Sprintf("Creating Rsync Transfer Service [%v/%v] for Stunnel connection "+
			"on destination MigCluster ", svc.Namespace, svc.Name))
		err = destClient.Create(context.TODO(), &svc)
		if k8serror.IsAlreadyExists(err) {
			t.Log.Info("Rsync transfer svc already exists on destination", "namespace", ns)
		} else if err != nil {
			return err
		}
		route := routev1.Route{
			ObjectMeta: metav1.ObjectMeta{
				Name:      DirectVolumeMigrationRsyncTransferRoute,
				Namespace: ns,
				Labels: map[string]string{
					"app": DirectVolumeMigrationRsyncTransfer,
				},
			},
			Spec: routev1.RouteSpec{
				To: routev1.RouteTargetReference{
					Kind: "Service",
					Name: DirectVolumeMigrationRsyncTransferSvc,
				},
				Port: &routev1.RoutePort{
					TargetPort: intstr.IntOrString{Type: intstr.Int, IntVal: 2222},
				},
				TLS: &routev1.TLSConfig{
					Termination: routev1.TLSTerminationPassthrough,
				},
			},
		}
		// Get cluster subdomain if it exists
		cluster, err := t.Owner.GetDestinationCluster(t.Client)
		if err != nil {
			return err
		}
		// Ignore error since this is optional config and won't break
		// anything if it doesn't exist
		subdomain, _ := cluster.GetClusterSubdomain(t.Client)

		// This is a backdoor setting to help guarantee DVM can still function if a
		// user is migrating namespaces that are 60+ characters
		// NOTE: We do no validation of this subdomain value. User is expected to
		// set this properly and it's only used for the unlikely case a user needs
		// to migrate namespaces with very long names.
		if subdomain != "" {
			// Ensure that route prefix will not exceed 63 chars
			// Route gen will add `-` between name + ns so need to ensure below is <62 chars
			// NOTE: only do this if we actually get a configured subdomain,
			// otherwise just use the name and hope for the best
			prefix := fmt.Sprintf("%s-%s", DirectVolumeMigrationRsyncTransferRoute, getMD5Hash(ns))
			if len(prefix) > 62 {
				prefix = prefix[0:62]
			}
			host := fmt.Sprintf("%s.%s", prefix, subdomain)
			route.Spec.Host = host
		}
		t.Log.Info(fmt.Sprintf("Creating Rsync Transfer Route [%v/%v] for Stunnel connection "+
			"on destination MigCluster ", route.Namespace, route.Name))
		err = destClient.Create(context.TODO(), &route)
		if k8serror.IsAlreadyExists(err) {
			t.Log.Info("Rsync transfer route already exists on destination", "namespace", ns)
		} else if err != nil {
			return err
		}
		t.RsyncRoutes[ns] = route.Spec.Host
	}
	return nil
}

// Transfer pod which runs rsyncd
func (t *Task) createRsyncTransferPods() error {
	// Ensure SSH Keys exist
	err := t.generateSSHKeys()
	if err != nil {
		return err
	}

	// Get client for destination
	destClient, err := t.getDestinationClient()
	if err != nil {
		return err
	}

	// Get transfer image
	cluster, err := t.Owner.GetDestinationCluster(t.Client)
	if err != nil {
		return err
	}
	t.Log.Info("Getting Rsync Transfer Pod image from ConfigMap.")
	transferImage, err := cluster.GetRsyncTransferImage(t.Client)
	if err != nil {
		return err
	}
	t.Log.Info("Getting Rsync Transfer Pod limits and requests from ConfigMap.")
	limits, requests, err := getPodResourceLists(t.Client, TRANSFER_POD_CPU_LIMIT, TRANSFER_POD_MEMORY_LIMIT, TRANSFER_POD_CPU_REQUEST, TRANSFER_POD_MEMORY_REQUEST)
	if err != nil {
		return err
	}
	// one transfer pod should be created per namespace and should mount all
	// PVCs that are being written to in that namespace

	// Transfer pod contains 2 containers, this is the stunnel container +
	// rsyncd

	// Transfer pod should also mount the stunnel configmap, the rsync secret
	// (contains creds), and add appropiate health checks for both stunnel +
	// rsyncd containers.

	// Generate pubkey bytes
	// TODO: Use a secret for this so we aren't regenerating every time
	t.Log.Info("Generating SSH public key for Rsync Transfer Pod")
	publicRsaKey, err := ssh.NewPublicKey(t.SSHKeys.PublicKey)
	if err != nil {
		return err
	}
	pubKeyBytes := ssh.MarshalAuthorizedKey(publicRsaKey)
	mode := int32(0600)

	isRsyncPrivileged, err := isRsyncPrivileged(destClient)
	if err != nil {
		return err
	}
	t.Log.Info(fmt.Sprintf("Rsync Transfer pod will be created with privileged=[%v]",
		isRsyncPrivileged))
	// Loop through namespaces and create transfer pod
	pvcMap := t.getPVCNamespaceMap()
	for ns, vols := range pvcMap {
		volumeMounts := []corev1.VolumeMount{}
		volumes := []corev1.Volume{
			{
				Name: "stunnel-conf",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: DirectVolumeMigrationStunnelConfig,
						},
					},
				},
			},
			{
				Name: "stunnel-certs",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: DirectVolumeMigrationStunnelCerts,
						Items: []corev1.KeyToPath{
							{
								Key:  "tls.crt",
								Path: "tls.crt",
							},
							{
								Key:  "ca.crt",
								Path: "ca.crt",
							},
							{
								Key:  "tls.key",
								Path: "tls.key",
							},
						},
					},
				},
			},
			{
				Name: "rsync-creds",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName:  DirectVolumeMigrationRsyncCreds,
						DefaultMode: &mode,
						Items: []corev1.KeyToPath{
							{
								Key:  "credentials",
								Path: "rsyncd.secrets",
							},
						},
					},
				},
			},
			{
				Name: "rsyncd-conf",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: DirectVolumeMigrationRsyncConfig,
						},
					},
				},
			},
		}
		trueBool := true
		runAsUser := int64(0)

		// Add PVC volume mounts
		for _, vol := range vols {
			volumeMounts = append(volumeMounts, corev1.VolumeMount{
				Name:      vol.Name,
				MountPath: fmt.Sprintf("/mnt/%s/%s", ns, vol.Name),
			})
			volumes = append(volumes, corev1.Volume{
				Name: vol.Name,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: vol.Name,
					},
				},
			})
		}
		// Add rsyncd config mount
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "rsyncd-conf",
			MountPath: "/etc/rsyncd.conf",
			SubPath:   "rsyncd.conf",
		})
		// Add rsync creds to volumeMounts
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "rsync-creds",
			MountPath: "/etc/rsyncd.secrets",
			SubPath:   "rsyncd.secrets",
		})

		dvmLabels := t.buildDVMLabels()
		dvmLabels["purpose"] = DirectVolumeMigrationRsync

		transferPod := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      DirectVolumeMigrationRsyncTransfer,
				Namespace: ns,
				Labels:    dvmLabels,
			},
			Spec: corev1.PodSpec{
				Volumes: volumes,
				Containers: []corev1.Container{
					{
						Name:  "rsyncd",
						Image: transferImage,
						Env: []corev1.EnvVar{
							{
								Name:  "SSH_PUBLIC_KEY",
								Value: string(pubKeyBytes),
							},
						},
						Command: []string{"/usr/bin/rsync", "--daemon", "--no-detach", "--port=22", "-vvv"},
						Ports: []corev1.ContainerPort{
							{
								Name:          "rsyncd",
								Protocol:      corev1.ProtocolTCP,
								ContainerPort: int32(22),
							},
						},
						VolumeMounts: volumeMounts,
						SecurityContext: &corev1.SecurityContext{
							Privileged:             &isRsyncPrivileged,
							RunAsUser:              &runAsUser,
							ReadOnlyRootFilesystem: &trueBool,
						},
						Resources: corev1.ResourceRequirements{
							Limits:   limits,
							Requests: requests,
						},
					},
					{
						Name:    DirectVolumeMigrationStunnel,
						Image:   transferImage,
						Command: []string{"/bin/stunnel", "/etc/stunnel/stunnel.conf"},
						Ports: []corev1.ContainerPort{
							{
								Name:          DirectVolumeMigrationStunnel,
								Protocol:      corev1.ProtocolTCP,
								ContainerPort: int32(2222),
							},
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "stunnel-conf",
								MountPath: "/etc/stunnel/stunnel.conf",
								SubPath:   "stunnel.conf",
							},
							{
								Name:      "stunnel-certs",
								MountPath: "/etc/stunnel/certs",
							},
						},
						SecurityContext: &corev1.SecurityContext{
							Privileged:             &isRsyncPrivileged,
							RunAsUser:              &runAsUser,
							ReadOnlyRootFilesystem: &trueBool,
						},
					},
				},
			},
		}
		t.Log.Info(fmt.Sprintf("Creating Rsync Transfer Pod [%v/%v] with containers [rsyncd, stunnel] on destination cluster.",
			transferPod.Namespace, transferPod.Name))
		err = destClient.Create(context.TODO(), &transferPod)
		if k8serror.IsAlreadyExists(err) {
			t.Log.Info("Rsync transfer pod already exists on destination", "namespace", transferPod.Namespace)
		} else if err != nil {
			return err
		}
		t.Log.Info("Rsync transfer pod created", "name", transferPod.Name, "namespace", transferPod.Namespace)

	}
	return nil
}

func getPodResourceLists(client k8sclient.Client, cpuLimit string, memoryLimit string, cpu_request string, memoryRequest string) (corev1.ResourceList, corev1.ResourceList, error) {
	podConfigMap := &corev1.ConfigMap{}
	err := client.Get(context.TODO(), types.NamespacedName{Name: "migration-controller", Namespace: migapi.OpenshiftMigrationNamespace}, podConfigMap)
	if err != nil {
		return nil, nil, err
	}
	limits := corev1.ResourceList{
		corev1.ResourceMemory: resource.MustParse("1Gi"),
		corev1.ResourceCPU:    resource.MustParse("1"),
	}
	if _, exists := podConfigMap.Data[cpuLimit]; exists {
		cpu := resource.MustParse(podConfigMap.Data[cpuLimit])
		limits[corev1.ResourceCPU] = cpu
	}
	if _, exists := podConfigMap.Data[memoryLimit]; exists {
		memory := resource.MustParse(podConfigMap.Data[memoryLimit])
		limits[corev1.ResourceMemory] = memory
	}
	requests := corev1.ResourceList{
		corev1.ResourceMemory: resource.MustParse("1Gi"),
		corev1.ResourceCPU:    resource.MustParse("400m"),
	}
	if _, exists := podConfigMap.Data[cpu_request]; exists {
		cpu := resource.MustParse(podConfigMap.Data[cpu_request])
		requests[corev1.ResourceCPU] = cpu
	}
	if _, exists := podConfigMap.Data[memoryRequest]; exists {
		memory := resource.MustParse(podConfigMap.Data[memoryRequest])
		requests[corev1.ResourceMemory] = memory
	}
	return limits, requests, nil
}

type pvcMapElement struct {
	Name   string
	Verify bool
}

func (t *Task) getPVCNamespaceMap() map[string][]pvcMapElement {
	nsMap := map[string][]pvcMapElement{}
	for _, pvc := range t.Owner.Spec.PersistentVolumeClaims {
		if vols, exists := nsMap[pvc.Namespace]; exists {
			vols = append(vols, pvcMapElement{Name: pvc.Name, Verify: pvc.Verify})
			nsMap[pvc.Namespace] = vols
		} else {
			nsMap[pvc.Namespace] = []pvcMapElement{pvcMapElement{Name: pvc.Name, Verify: pvc.Verify}}
		}
	}
	return nsMap
}

func (t *Task) getRsyncRoute(namespace string) (string, error) {
	// Get client for destination
	destClient, err := t.getDestinationClient()
	if err != nil {
		return "", err
	}
	route := routev1.Route{}

	key := types.NamespacedName{Name: DirectVolumeMigrationRsyncTransferRoute, Namespace: namespace}
	err = destClient.Get(context.TODO(), key, &route)
	if err != nil {
		return "", err
	}
	return route.Spec.Host, nil
}

func (t *Task) areRsyncRoutesAdmitted() (bool, []string, error) {
	messages := []string{}
	// Get client for destination
	destClient, err := t.getDestinationClient()
	if err != nil {
		return false, messages, err
	}
	nsMap := t.getPVCNamespaceMap()
	for namespace, _ := range nsMap {
		route := routev1.Route{}

		key := types.NamespacedName{Name: DirectVolumeMigrationRsyncTransferRoute, Namespace: namespace}
		err = destClient.Get(context.TODO(), key, &route)
		if err != nil {
			return false, messages, err
		}
		admitted := false
		message := "no status condition available for the route"
		// Check if we can find the admitted condition for the route
		for _, ingress := range route.Status.Ingress {
			for _, condition := range ingress.Conditions {
				if condition.Type == routev1.RouteAdmitted && condition.Status == corev1.ConditionFalse {
					t.Log.Info(fmt.Sprintf("Rsync Transfer Route [%v/%v] has not been admitted.",
						route.Namespace, route.Name))
					admitted = false
					message = condition.Message
					break
				}
				if condition.Type == routev1.RouteAdmitted && condition.Status == corev1.ConditionTrue {
					t.Log.Info(fmt.Sprintf("Rsync Transfer Route [%v/%v] has been admitted successfully.",
						route.Namespace, route.Name))
					admitted = true
					break
				}
			}
		}
		if !admitted {
			messages = append(messages, message)
		}
	}
	if len(messages) > 0 {
		return false, messages, nil
	}
	return true, []string{}, nil
}

func (t *Task) createRsyncPassword() (string, error) {
	var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	random.Seed(time.Now().UnixNano())
	password := make([]byte, 6)
	for i := range password {
		password[i] = letters[random.Intn(len(letters))]
	}

	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: migapi.OpenshiftMigrationNamespace,
			Name:      DirectVolumeMigrationRsyncPass,
			Labels: map[string]string{
				"app": DirectVolumeMigrationRsyncTransfer,
			},
		},
		StringData: map[string]string{
			corev1.BasicAuthPasswordKey: string(password),
		},
		Type: corev1.SecretTypeBasicAuth,
	}
	t.Log.Info(fmt.Sprintf("Creating Rsync Password Secret [%v/%v] on host cluster",
		secret.Namespace, secret.Name))
	err := t.Client.Create(context.TODO(), &secret)
	if k8serror.IsAlreadyExists(err) {
		t.Log.Info("Secret already exists on host", "name", "directvolumemigration-rsync-pass", "namespace", migapi.OpenshiftMigrationNamespace)
	} else if err != nil {
		return "", err
	}
	return string(password), nil
}

func (t *Task) getRsyncPassword() (string, error) {
	rsyncSecret := corev1.Secret{}
	key := types.NamespacedName{Name: DirectVolumeMigrationRsyncPass, Namespace: migapi.OpenshiftMigrationNamespace}
	t.Log.Info(fmt.Sprintf("Getting Rsync Password from Secret [%v/%v] on host cluster",
		rsyncSecret.Namespace, rsyncSecret.Name))
	err := t.Client.Get(context.TODO(), key, &rsyncSecret)
	if k8serror.IsNotFound(err) {
		t.Log.Info("Secret is not found", "name", DirectVolumeMigrationRsyncPass, "namespace", migapi.OpenshiftMigrationNamespace)
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if pass, ok := rsyncSecret.Data[corev1.BasicAuthPasswordKey]; ok {
		return string(pass), nil
	}
	return "", nil
}

func (t *Task) deleteRsyncPassword() error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: migapi.OpenshiftMigrationNamespace,
			Name:      DirectVolumeMigrationRsyncPass,
		},
	}
	t.Log.Info(fmt.Sprintf("Deleting Rsync password Secret [%v/%v] on host MigCluster",
		secret.Namespace, secret.Name))
	err := t.Client.Delete(context.TODO(), secret, k8sclient.PropagationPolicy(metav1.DeletePropagationBackground))
	if k8serror.IsNotFound(err) {
		t.Log.Info("Secret is not found", "name", DirectVolumeMigrationRsyncPass, "namespace", migapi.OpenshiftMigrationNamespace)
	} else if err != nil {
		return err
	}
	return nil
}

//Returns a map of PVCNamespacedName to the pod.NodeName
func (t *Task) getPVCNodeNameMap() (map[string]string, error) {
	nodeNameMap := map[string]string{}
	pvcMap := t.getPVCNamespaceMap()

	srcClient, err := t.getSourceClient()
	if err != nil {
		return nil, err
	}

	for ns, _ := range pvcMap {

		nsPodList := corev1.PodList{}
		err = srcClient.List(context.TODO(), k8sclient.InNamespace(ns), &nsPodList)
		if err != nil {
			return nil, err
		}

		for _, pod := range nsPodList.Items {
			if pod.Status.Phase == corev1.PodRunning {
				for _, vol := range pod.Spec.Volumes {
					if vol.PersistentVolumeClaim != nil {
						pvcNsName := pod.ObjectMeta.Namespace + "/" + vol.PersistentVolumeClaim.ClaimName
						nodeNameMap[pvcNsName] = pod.Spec.NodeName
					}
				}
			}
		}
	}

	return nodeNameMap, nil
}

// validates extra Rsync options set by user
// only returns options identified as valid
func (t *Task) filterRsyncExtraOptions(options []string) (validatedOptions []string) {
	for _, opt := range options {
		if valid, _ := regexp.Match(`^\-{1,2}[\w-]+?\w$`, []byte(opt)); valid {
			validatedOptions = append(validatedOptions, opt)
		} else {
			t.Log.Info(fmt.Sprintf("Invalid Rsync extra option passed: %s", opt))
		}
	}
	return
}

// generates Rsync options based on custom options provided by the user in MigrationController CR
func (t *Task) getRsyncOptions() []string {
	var rsyncOpts []string
	defaultInfoOpts := "COPY2,DEL2,REMOVE2,SKIP2,FLIST2,PROGRESS2,STATS2"
	defaultExtraOpts := []string{
		"--human-readable",
		"--port", "2222",
		"--log-file", "/dev/stdout",
	}
	rsyncOptions := migsettings.Settings.RsyncOpts
	if rsyncOptions.BwLimit != -1 {
		rsyncOpts = append(rsyncOpts,
			fmt.Sprintf("--bwlimit=%d", rsyncOptions.BwLimit))
	}
	if rsyncOptions.Archive {
		rsyncOpts = append(rsyncOpts, "--archive")
	}
	if rsyncOptions.Delete {
		rsyncOpts = append(rsyncOpts, "--delete")
		// --delete option does not work without --recursive
		rsyncOpts = append(rsyncOpts, "--recursive")
	}
	if rsyncOptions.HardLinks {
		rsyncOpts = append(rsyncOpts, "--hard-links")
	}
	if rsyncOptions.Partial {
		rsyncOpts = append(rsyncOpts, "--partial")
	}
	if valid, _ := regexp.Match(`^\w[\w,]*?\w$`, []byte(rsyncOptions.Info)); valid {
		rsyncOpts = append(rsyncOpts,
			fmt.Sprintf("--info=%s", rsyncOptions.Info))
	} else {
		rsyncOpts = append(rsyncOpts,
			fmt.Sprintf("--info=%s", defaultInfoOpts))
	}
	rsyncOpts = append(rsyncOpts, defaultExtraOpts...)
	rsyncOpts = append(rsyncOpts,
		t.filterRsyncExtraOptions(rsyncOptions.Extras)...)
	return rsyncOpts
}

type PVCWithSecurityContext struct {
	name               string
	fsGroup            *int64
	supplementalGroups []int64
	seLinuxOptions     *corev1.SELinuxOptions
	verify             bool

	// TODO:
	// add capabilities for dvm controller to handle case the source
	// application pods is privileged with the following flags from
	// PodSecurityContext and Containers' SecurityContext
	// We need to
	// 1. go through the pod's volume.
	// 2. find the container where it is volume mounted.
	//    i. if the container's fields are non-nil, use that
	//    ii. if the container's fields are nil, use the pods fields.

	// RunAsUser  *int64
	// RunAsGroup *int64
}

// Get fsGroup per PVC
func (t *Task) getfsGroupMapForNamespace() (map[string][]PVCWithSecurityContext, error) {
	pvcMap := t.getPVCNamespaceMap()
	pvcSecurityContextMap := map[string][]PVCWithSecurityContext{}
	for ns, _ := range pvcMap {
		pvcSecurityContextMap[ns] = []PVCWithSecurityContext{}
	}
	for ns, pvcs := range pvcMap {
		srcClient, err := t.getSourceClient()
		if err != nil {
			return nil, err
		}
		podList := &corev1.PodList{}
		err = srcClient.List(context.TODO(), &k8sclient.ListOptions{Namespace: ns}, podList)
		if err != nil {
			return nil, err
		}

		// for each namespace, have a pvc->SCC map to look up in the pvc loop later
		// we will use the scc of the last pod in the list mounting the pvc
		pvcSecurityContextMapForNamespace := map[string]PVCWithSecurityContext{}
		for _, pod := range podList.Items {
			for _, vol := range pod.Spec.Volumes {
				if vol.PersistentVolumeClaim != nil {
					pvcSecurityContextMapForNamespace[vol.PersistentVolumeClaim.ClaimName] = PVCWithSecurityContext{
						name:               vol.PersistentVolumeClaim.ClaimName,
						fsGroup:            pod.Spec.SecurityContext.FSGroup,
						supplementalGroups: pod.Spec.SecurityContext.SupplementalGroups,
						seLinuxOptions:     pod.Spec.SecurityContext.SELinuxOptions,
					}
				}
			}
		}

		for _, claim := range pvcs {
			pss, exists := pvcSecurityContextMapForNamespace[claim.Name]
			if exists {
				pss.verify = claim.Verify
				pvcSecurityContextMap[ns] = append(pvcSecurityContextMap[ns], pss)
				continue
			}
			// pvc not used by any pod
			pvcSecurityContextMap[ns] = append(pvcSecurityContextMap[ns], PVCWithSecurityContext{
				name:               claim.Name,
				fsGroup:            nil,
				supplementalGroups: nil,
				seLinuxOptions:     nil,
				verify:             claim.Verify,
			})
		}
	}
	return pvcSecurityContextMap, nil
}

func isClaimUsedByPod(claimName string, p *corev1.Pod) bool {
	for _, vol := range p.Spec.Volumes {
		if vol.PersistentVolumeClaim != nil && vol.PersistentVolumeClaim.ClaimName == claimName {
			return true
		}
	}
	return false
}

// Create rsync client pods
func (t *Task) createRsyncClientPods() error {
	// Get client for destination
	srcClient, err := t.getSourceClient()
	if err != nil {
		return err
	}

	// Get transfer image for cluster
	cluster, err := t.Owner.GetSourceCluster(t.Client)
	if err != nil {
		return err
	}
	t.Log.Info("Getting image for Rsync client Pods that will be created on source MigCluster")
	transferImage, err := cluster.GetRsyncTransferImage(t.Client)
	if err != nil {
		return err
	}
	t.Log.Info(fmt.Sprintf("Using image [%v] for Rsync client Pods", transferImage))

	t.Log.Info("Getting [NS => PVCWithSecurityContext] mappings for PVCs to be migrated")
	pvcMap, err := t.getfsGroupMapForNamespace()
	if err != nil {
		return err
	}

	t.Log.Info("Getting Rsync password for Rsync client Pods")
	password, err := t.getRsyncPassword()
	if err != nil {
		return err
	}

	t.Log.Info("Getting [PVC => NodeName] mapping for PVCs to be migrated")
	pvcNodeMap, err := t.getPVCNodeNameMap()
	if err != nil {
		return err
	}

	t.Log.Info("Getting limits and requests for Rsync client Pods")
	limits, requests, err := getPodResourceLists(t.Client, CLIENT_POD_CPU_LIMIT, CLIENT_POD_MEMORY_LIMIT, CLIENT_POD_CPU_REQUEST, CLIENT_POD_MEMORY_REQUEST)
	if err != nil {
		return err
	}

	isPrivileged, err := isRsyncPrivileged(srcClient)
	t.Log.Info(fmt.Sprintf("Rsync client Pods will be created with privileged=[%v]", isPrivileged))

	for ns, vols := range pvcMap {
		// Get stunnel svc IP
		svc := corev1.Service{}
		key := types.NamespacedName{Name: DirectVolumeMigrationRsyncTransferSvc, Namespace: ns}
		err := srcClient.Get(context.TODO(), key, &svc)
		if err != nil {
			return err
		}
		ip := svc.Spec.ClusterIP

		trueBool := true
		runAsUser := int64(0)

		// Add PVC volume mounts
		for _, vol := range vols {
			volumes := []corev1.Volume{}
			volumeMounts := []corev1.VolumeMount{}
			containers := []corev1.Container{}
			volumeMounts = append(volumeMounts, corev1.VolumeMount{
				Name:      vol.name,
				MountPath: fmt.Sprintf("/mnt/%s/%s", ns, vol.name),
			})
			volumes = append(volumes, corev1.Volume{
				Name: vol.name,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: vol.name,
					},
				},
			})
			rsyncCommand := []string{"rsync"}
			rsyncCommand = append(rsyncCommand, t.getRsyncOptions()...)
			if vol.verify {
				rsyncCommand = append(rsyncCommand, "--checksum")
			}
			rsyncCommand = append(rsyncCommand, fmt.Sprintf("/mnt/%s/%s/", ns, vol.name))
			rsyncCommand = append(rsyncCommand, fmt.Sprintf("rsync://root@%s/%s", ip, vol.name))
			t.Log.Info(fmt.Sprintf("Container [%v] will use Rsync command [%s]",
				DirectVolumeMigrationRsyncClient, strings.Join(rsyncCommand, " ")))
			containers = append(containers, corev1.Container{
				Name:  DirectVolumeMigrationRsyncClient,
				Image: transferImage,
				Env: []corev1.EnvVar{
					{
						Name:  "RSYNC_PASSWORD",
						Value: password,
					},
				},
				TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
				Command:                  rsyncCommand,
				Ports: []corev1.ContainerPort{
					{
						Name:          DirectVolumeMigrationRsyncClient,
						Protocol:      corev1.ProtocolTCP,
						ContainerPort: int32(22),
					},
				},
				VolumeMounts: volumeMounts,
				SecurityContext: &corev1.SecurityContext{
					Privileged:             &isPrivileged,
					RunAsUser:              &runAsUser,
					ReadOnlyRootFilesystem: &trueBool,
				},
				Resources: corev1.ResourceRequirements{
					Limits:   limits,
					Requests: requests,
				},
			})
			clientPod := corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("directvolumemigration-rsync-transfer-%s", vol.name),
					Namespace: ns,
					Labels: map[string]string{
						"app":                   DirectVolumeMigrationRsyncTransfer,
						"directvolumemigration": DirectVolumeMigrationRsyncClient,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Volumes:       volumes,
					Containers:    containers,
					NodeName:      pvcNodeMap[ns+"/"+vol.name],
					SecurityContext: &corev1.PodSecurityContext{
						SupplementalGroups: vol.supplementalGroups,
						FSGroup:            vol.fsGroup,
						SELinuxOptions:     vol.seLinuxOptions,
					},
				},
			}
			t.Log.Info(fmt.Sprintf("Creating Rsync client Pod [%v/%v] on source cluster.",
				clientPod.Namespace, clientPod.Name))
			err = srcClient.Create(context.TODO(), &clientPod)
			if k8serror.IsAlreadyExists(err) {
				t.Log.Info("Rsync client pod already exists on source", "namespace", clientPod.Namespace)
			} else if err != nil {
				return err
			}
			t.Log.Info("Rsync client pod created", "name", clientPod.Name, "namespace", clientPod.Namespace)
		}

	}
	return nil
}

func isRsyncPrivileged(client compat.Client) (bool, error) {
	cm := &corev1.ConfigMap{}
	err := client.Get(context.TODO(), k8sclient.ObjectKey{Name: migapi.ClusterConfigMapName, Namespace: migapi.OpenshiftMigrationNamespace}, cm)
	if err != nil {
		return false, err
	}
	if cm.Data != nil {
		isRsyncPrivileged, exists := cm.Data["RSYNC_PRIVILEGED"]
		if !exists {
			return false, fmt.Errorf("RSYNC_PRIVILEGED boolean does not exist. Verify source and destination clusters operators are up to date")
		}
		parsed, err := strconv.ParseBool(isRsyncPrivileged)
		if err != nil {
			return false, err
		}
		return parsed, nil
	}
	return false, fmt.Errorf("configmap %s of source cluster has empty data", k8sclient.ObjectKey{Name: migapi.ClusterConfigMapName, Namespace: migapi.OpenshiftMigrationNamespace}.String())
}

// Create rsync PV progress CR on destination cluster
func (t *Task) createPVProgressCR() error {
	pvcMap := t.getPVCNamespaceMap()
	labels := t.Owner.GetCorrelationLabels()
	for ns, vols := range pvcMap {
		for _, vol := range vols {
			dvmp := migapi.DirectVolumeMigrationProgress{
				ObjectMeta: metav1.ObjectMeta{
					Name:      getMD5Hash(t.Owner.Name + vol.Name + ns),
					Labels:    labels,
					Namespace: migapi.OpenshiftMigrationNamespace,
				},
				Spec: migapi.DirectVolumeMigrationProgressSpec{
					ClusterRef: t.Owner.Spec.SrcMigClusterRef,
					PodRef: &corev1.ObjectReference{
						Namespace: ns,
						Name:      fmt.Sprintf("directvolumemigration-rsync-transfer-%s", vol.Name),
					},
				},
				Status: migapi.DirectVolumeMigrationProgressStatus{},
			}
			migapi.SetOwnerReference(t.Owner, t.Owner, &dvmp)
			t.Log.Info(fmt.Sprintf("Creating DVMP [%v/%v] on [host] MigCluster to track Rsync Pod [%v/%v] completion on MigCluster [%v/%v].",
				dvmp.Namespace, dvmp.Name, dvmp.Spec.PodRef.Namespace, dvmp.Spec.PodRef.Name,
				t.Owner.Spec.SrcMigClusterRef.Namespace, t.Owner.Spec.SrcMigClusterRef.Name))
			err := t.Client.Create(context.TODO(), &dvmp)
			if k8serror.IsAlreadyExists(err) {
				t.Log.Info("Rsync client progress CR already exists on destination", "namespace", dvmp.Namespace, "name", dvmp.Name)
			} else if err != nil {
				return err
			}
			t.Log.Info("Rsync client progress CR created", "name", dvmp.Name, "namespace", dvmp.Namespace)
		}

	}
	return nil
}

func getMD5Hash(s string) string {
	hash := md5.Sum([]byte(s))
	return hex.EncodeToString(hash[:])
}

func (t *Task) haveRsyncClientPodsCompletedOrFailed() (bool, bool, error) {
	t.Owner.Status.RunningPods = []*migapi.PodProgress{}
	t.Owner.Status.FailedPods = []*migapi.PodProgress{}
	t.Owner.Status.SuccessfulPods = []*migapi.PodProgress{}
	t.Owner.Status.PendingPods = []*migapi.PodProgress{}
	var pendingPods []string
	var runningPods []string
	pvcMap := t.getPVCNamespaceMap()
	for ns, vols := range pvcMap {
		for _, vol := range vols {
			dvmp := migapi.DirectVolumeMigrationProgress{}
			err := t.Client.Get(context.TODO(), types.NamespacedName{
				Name:      getMD5Hash(t.Owner.Name + vol.Name + ns),
				Namespace: migapi.OpenshiftMigrationNamespace,
			}, &dvmp)
			if err != nil {
				// todo, need to start thinking about collecting this error and reporting other CR's progress
				return false, false, err
			}
			objRef := &corev1.ObjectReference{
				Namespace: ns,
				Name:      fmt.Sprintf("directvolumemigration-rsync-transfer-%s", vol.Name),
			}
			switch {
			case dvmp.Status.PodPhase == corev1.PodRunning:
				t.Owner.Status.RunningPods = append(t.Owner.Status.RunningPods, &migapi.PodProgress{
					ObjectReference:             objRef,
					LastObservedProgressPercent: dvmp.Status.LastObservedProgressPercent,
					LastObservedTransferRate:    dvmp.Status.LastObservedTransferRate,
				})
				runningPods = append(runningPods, fmt.Sprintf("%s/%s", objRef.Name, objRef.Namespace))
			case dvmp.Status.PodPhase == corev1.PodFailed:
				t.Owner.Status.FailedPods = append(t.Owner.Status.FailedPods, &migapi.PodProgress{
					ObjectReference:             objRef,
					LastObservedProgressPercent: dvmp.Status.LastObservedProgressPercent,
					LastObservedTransferRate:    dvmp.Status.LastObservedTransferRate,
				})
			case dvmp.Status.PodPhase == corev1.PodSucceeded:
				t.Owner.Status.SuccessfulPods = append(t.Owner.Status.SuccessfulPods, &migapi.PodProgress{
					ObjectReference:             objRef,
					LastObservedProgressPercent: dvmp.Status.LastObservedProgressPercent,
					LastObservedTransferRate:    dvmp.Status.LastObservedTransferRate,
				})
			case dvmp.Status.PodPhase == corev1.PodPending:
				t.Owner.Status.PendingPods = append(t.Owner.Status.PendingPods, &migapi.PodProgress{
					ObjectReference:             objRef,
					LastObservedProgressPercent: dvmp.Status.LastObservedProgressPercent,
					LastObservedTransferRate:    dvmp.Status.LastObservedTransferRate,
				})
				pendingPods = append(pendingPods, fmt.Sprintf("%s/%s", objRef.Name, objRef.Namespace))
			}
		}
	}

	isCompleted := len(t.Owner.Status.SuccessfulPods)+len(t.Owner.Status.FailedPods) == len(t.Owner.Spec.PersistentVolumeClaims)
	hasAnyFailed := len(t.Owner.Status.FailedPods) > 0
	isAnyPending := len(t.Owner.Status.PendingPods) > 0
	isAnyRunning := len(t.Owner.Status.RunningPods) > 0
	if isAnyPending {
		pendingMessage := fmt.Sprintf("Rsync Client Pods [%s] are stuck in Pending state for more than 10 mins", strings.Join(pendingPods[:], ", "))
		t.Log.Info(pendingMessage)
		t.Owner.Status.SetCondition(migapi.Condition{
			Type:     RsyncClientPodsPending,
			Status:   migapi.True,
			Reason:   "PodStuckInContainerCreating",
			Category: migapi.Warn,
			Message:  pendingMessage,
		})
	}
	if isAnyRunning {
		t.Log.Info(fmt.Sprintf("Rsync Client Pods [%v] are running. Waiting for completion.", strings.Join(runningPods[:], ", ")))
	}
	return isCompleted, hasAnyFailed, nil
}

func hasAllRsyncClientPodsTimedOut(pvcMap map[string][]pvcMapElement, client k8sclient.Client, dvmName string) (bool, error) {
	for ns, vols := range pvcMap {
		for _, vol := range vols {
			dvmp := migapi.DirectVolumeMigrationProgress{}
			err := client.Get(context.TODO(), types.NamespacedName{
				Name:      getMD5Hash(dvmName + vol.Name + ns),
				Namespace: migapi.OpenshiftMigrationNamespace,
			}, &dvmp)
			if err != nil {
				return false, err
			}
			if dvmp.Status.PodPhase != corev1.PodFailed ||
				dvmp.Status.ContainerElapsedTime == nil ||
				(dvmp.Status.ContainerElapsedTime != nil &&
					dvmp.Status.ContainerElapsedTime.Duration.Round(time.Second).Seconds() != float64(DefaultStunnelTimeout)) {
				return false, nil
			}
		}
	}
	return true, nil
}

func isAllRsyncClientPodsNoRouteToHost(pvcMap map[string][]pvcMapElement, client k8sclient.Client, dvmName string) (bool, error) {
	for ns, vols := range pvcMap {
		for _, vol := range vols {
			dvmp := migapi.DirectVolumeMigrationProgress{}
			err := client.Get(context.TODO(), types.NamespacedName{
				Name:      getMD5Hash(dvmName + vol.Name + ns),
				Namespace: migapi.OpenshiftMigrationNamespace,
			}, &dvmp)
			if err != nil {
				return false, err
			}

			if dvmp.Status.PodPhase != corev1.PodFailed ||
				dvmp.Status.ContainerElapsedTime == nil ||
				(dvmp.Status.ContainerElapsedTime != nil &&
					dvmp.Status.ContainerElapsedTime.Duration.Seconds() > float64(5)) || *dvmp.Status.ExitCode != int32(10) || !strings.Contains(dvmp.Status.LogMessage, "No route to host") {
				return false, nil
			}
		}
	}
	return true, nil
}

// Delete rsync resources
func (t *Task) deleteRsyncResources() error {
	// Get client for source + destination
	srcClient, err := t.getSourceClient()
	if err != nil {
		return err
	}
	destClient, err := t.getDestinationClient()
	if err != nil {
		return err
	}

	t.Log.Info(fmt.Sprintf("Checking for stale Rsync resources on source MigCluster [%v/%v]",
		t.PlanResources.SrcMigCluster.Namespace, t.PlanResources.SrcMigCluster.Name))
	err = t.findAndDeleteResources(srcClient)
	if err != nil {
		return err
	}

	t.Log.Info(fmt.Sprintf("Checking for stale Rsync resources on destination MigCluster [%v/%v]",
		t.PlanResources.DestMigCluster.Namespace, t.PlanResources.DestMigCluster.Name))
	err = t.findAndDeleteResources(destClient)
	if err != nil {
		return err
	}

	err = t.deleteRsyncPassword()
	if err != nil {
		return err
	}

	if !t.Owner.Spec.DeleteProgressReportingCRs {
		return nil
	}

	t.Log.Info("Checking for stale DVMP resources on MigCluster [host]")
	err = t.deleteProgressReportingCRs(t.Client)
	if err != nil {
		return err
	}

	return nil
}

func (t *Task) waitForRsyncResourcesDeleted() (error, bool) {
	srcClient, err := t.getSourceClient()
	if err != nil {
		return err, false
	}
	destClient, err := t.getDestinationClient()
	if err != nil {
		return err, false
	}
	t.Log.Info("Checking if Rsync resource deletion has completed on source MigCluster")
	err, deleted := t.areRsyncResourcesDeleted(srcClient)
	if err != nil {
		return err, false
	}
	if !deleted {
		return nil, false
	}
	t.Log.Info("Checking if Rsync resource deletion has completed on destination MigCluster")
	err, deleted = t.areRsyncResourcesDeleted(destClient)
	if err != nil {
		return err, false
	}
	if !deleted {
		return nil, false
	}
	return nil, true
}

func (t *Task) areRsyncResourcesDeleted(client compat.Client) (error, bool) {
	selector := labels.SelectorFromSet(map[string]string{
		"app": DirectVolumeMigrationRsyncTransfer,
	})
	pvcMap := t.getPVCNamespaceMap()
	for ns, _ := range pvcMap {
		t.Log.Info(fmt.Sprintf("Searching namespace=[%v] for leftover Rsync Pods, ConfigMaps, "+
			"Services, Secrets, Routes with label [app: %v]", ns, DirectVolumeMigrationRsyncTransfer))
		podList := corev1.PodList{}
		cmList := corev1.ConfigMapList{}
		svcList := corev1.ServiceList{}
		secretList := corev1.SecretList{}
		routeList := routev1.RouteList{}

		// Get Pod list
		err := client.List(
			context.TODO(),
			&k8sclient.ListOptions{
				Namespace:     ns,
				LabelSelector: selector,
			},
			&podList)
		if err != nil {
			return err, false
		}
		if len(podList.Items) > 0 {
			t.Log.Info(fmt.Sprintf("Found stale Rsync Pod [%v/%v] Phase=[%v].",
				podList.Items[0].Namespace, podList.Items[0].Name, podList.Items[0].Status.Phase))
			return nil, false
		}
		// Get Secret list
		err = client.List(
			context.TODO(),
			&k8sclient.ListOptions{
				Namespace:     ns,
				LabelSelector: selector,
			},
			&secretList)
		if err != nil {
			return err, false
		}
		if len(secretList.Items) > 0 {
			t.Log.Info(fmt.Sprintf("Found stale Rsync Secret [%v/%v].",
				secretList.Items[0].Namespace, secretList.Items[0].Name))
			return nil, false
		}
		// Get configmap list
		err = client.List(
			context.TODO(),
			&k8sclient.ListOptions{
				Namespace:     ns,
				LabelSelector: selector,
			},
			&cmList)
		if err != nil {
			return err, false
		}
		if len(cmList.Items) > 0 {
			t.Log.Info(fmt.Sprintf("Found stale Rsync ConfigMap [%v/%v]",
				cmList.Items[0].Namespace, cmList.Items[0].Name))
			return nil, false
		}
		// Get svc list
		err = client.List(
			context.TODO(),
			&k8sclient.ListOptions{
				Namespace:     ns,
				LabelSelector: selector,
			},
			&svcList)
		if err != nil {
			return err, false
		}
		if len(svcList.Items) > 0 {
			t.Log.Info(fmt.Sprintf("Found stale Rsync Service [%v/%v]",
				svcList.Items[0].Namespace, svcList.Items[0].Name))
			return nil, false
		}

		// Get route list
		err = client.List(
			context.TODO(),
			&k8sclient.ListOptions{
				Namespace:     ns,
				LabelSelector: selector,
			},
			&routeList)
		if err != nil {
			return err, false
		}
		if len(routeList.Items) > 0 {
			t.Log.Info(fmt.Sprintf("Found stale Rsync Route [%v/%v]",
				routeList.Items[0].Namespace, routeList.Items[0].Name))
			return nil, false
		}
	}
	return nil, true

}

func (t *Task) findAndDeleteResources(client compat.Client) error {
	// Find all resources with the app label
	// TODO: This label set should include a DVM run-specific UID.
	selector := labels.SelectorFromSet(map[string]string{
		"app": DirectVolumeMigrationRsyncTransfer,
	})
	pvcMap := t.getPVCNamespaceMap()
	for ns, _ := range pvcMap {
		podList := corev1.PodList{}
		cmList := corev1.ConfigMapList{}
		svcList := corev1.ServiceList{}
		secretList := corev1.SecretList{}
		routeList := routev1.RouteList{}

		// Get Pod list
		err := client.List(
			context.TODO(),
			&k8sclient.ListOptions{
				Namespace:     ns,
				LabelSelector: selector,
			},
			&podList)
		if err != nil {
			return err
		}
		// Get Secret list
		err = client.List(
			context.TODO(),
			&k8sclient.ListOptions{
				Namespace:     ns,
				LabelSelector: selector,
			},
			&secretList)
		if err != nil {
			return err
		}

		// Get configmap list
		err = client.List(
			context.TODO(),
			&k8sclient.ListOptions{
				Namespace:     ns,
				LabelSelector: selector,
			},
			&cmList)
		if err != nil {
			return err
		}

		// Get svc list
		err = client.List(
			context.TODO(),
			&k8sclient.ListOptions{
				Namespace:     ns,
				LabelSelector: selector,
			},
			&svcList)
		if err != nil {
			return err
		}

		// Get route list
		err = client.List(
			context.TODO(),
			&k8sclient.ListOptions{
				Namespace:     ns,
				LabelSelector: selector,
			},
			&routeList)
		if err != nil {
			return err
		}

		// Delete pods
		for _, pod := range podList.Items {
			t.Log.Info(fmt.Sprintf("DELETING stale DVM Pod [%v/%v]", pod.Namespace, pod.Name))
			err = client.Delete(context.TODO(), &pod, k8sclient.PropagationPolicy(metav1.DeletePropagationBackground))
			if err != nil && !k8serror.IsNotFound(err) {
				return err
			}
		}

		// Delete secrets
		for _, secret := range secretList.Items {
			t.Log.Info(fmt.Sprintf("DELETING stale DVM Secret [%v/%v]", secret.Namespace, secret.Name))
			err = client.Delete(context.TODO(), &secret, k8sclient.PropagationPolicy(metav1.DeletePropagationBackground))
			if err != nil && !k8serror.IsNotFound(err) {
				return err
			}
		}

		// Delete routes
		for _, route := range routeList.Items {
			t.Log.Info(fmt.Sprintf("DELETING stale DVM Route [%v/%v]", route.Namespace, route.Name))
			err = client.Delete(context.TODO(), &route, k8sclient.PropagationPolicy(metav1.DeletePropagationBackground))
			if err != nil && !k8serror.IsNotFound(err) {
				return err
			}
		}

		// Delete svcs
		for _, svc := range svcList.Items {
			t.Log.Info(fmt.Sprintf("DELETING stale DVM Service [%v/%v]", svc.Namespace, svc.Name))
			err = client.Delete(context.TODO(), &svc, k8sclient.PropagationPolicy(metav1.DeletePropagationBackground))
			if err != nil && !k8serror.IsNotFound(err) {
				return err
			}
		}

		// Delete configmaps
		for _, cm := range cmList.Items {
			t.Log.Info(fmt.Sprintf("DELETING stale DVM ConfigMap [%v/%v]", cm.Namespace, cm.Name))
			err = client.Delete(context.TODO(), &cm, k8sclient.PropagationPolicy(metav1.DeletePropagationBackground))
			if err != nil && !k8serror.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

func (t *Task) deleteProgressReportingCRs(client k8sclient.Client) error {
	pvcMap := t.getPVCNamespaceMap()

	for ns, vols := range pvcMap {
		for _, vol := range vols {
			dvmpName := getMD5Hash(t.Owner.Name + vol.Name + ns)
			t.Log.Info(fmt.Sprintf("DELETING stale DVMP CR [%v/%v]", dvmpName, ns))
			err := client.Delete(context.TODO(), &migapi.DirectVolumeMigrationProgress{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dvmpName,
					Namespace: ns,
				},
			}, k8sclient.PropagationPolicy(metav1.DeletePropagationBackground))
			if err != nil && !k8serror.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}
