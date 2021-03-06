package directvolumemigration

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"text/template"

	liberr "github.com/konveyor/controller/pkg/error"
	"github.com/konveyor/mig-controller/pkg/settings"
	"gopkg.in/yaml.v2"

	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"

	//"encoding/asn1"
	"encoding/pem"
	"math/big"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	//"k8s.io/apimachinery/pkg/types"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type stunnelConfig struct {
	Name          string
	Namespace     string
	StunnelPort   int32
	RsyncRoute    string
	RsyncPort     int32
	VerifyCA      bool
	VerifyCALevel string
	stunnelProxyConfig
}

type stunnelProxyConfig struct {
	ProxyHost     string
	ProxyUsername string
	ProxyPassword string
}

// TODO: Parameterize this more to support custom
// networking configs from directvolumemigration spec
const stunnelClientConfigTemplate = `apiVersion: v1
kind: ConfigMap
metadata:
  labels:
    purpose: stunnel
data:
  stunnel.conf: |
    foreground = yes
    pid =
    sslVersion = TLSv1.2
    client = yes
    syslog = no
    [rsync]
    accept = {{ .StunnelPort }}
    CAFile = /etc/stunnel/certs/ca.crt
    cert = /etc/stunnel/certs/tls.crt
{{ if not (eq .ProxyHost "") }}
    protocol = connect
    connect = {{ .ProxyHost }}
    protocolHost = {{ .RsyncRoute }}:443
{{ if not (eq .ProxyUsername "") }}
    protocolUsername = {{ .ProxyUsername }}
{{ end }}
{{ if not (eq .ProxyPassword "") }}
    protocolPassword = {{ .ProxyPassword }}
{{ end }}
{{ else }}
    connect = {{ .RsyncRoute }}:443
{{ end }}
{{ if .VerifyCA }}
    verify = {{ .VerifyCALevel }}
{{ end }}
    key = /etc/stunnel/certs/tls.key
    debug = 7
`

const stunnelDestinationConfigTemplate = `apiVersion: v1
kind: ConfigMap
metadata:
  labels:
    purpose: stunnel-config
data:
  stunnel.conf: |
    foreground = yes
    pid =
    socket = l:TCP_NODELAY=1
    socket = r:TCP_NODELAY=1
    debug = 7
    sslVersion = TLSv1.2

    [rsync]
    accept = {{ .StunnelPort }}
    connect = {{ .RsyncPort }}
    key = /etc/stunnel/certs/tls.key
    cert = /etc/stunnel/certs/tls.crt
    TIMEOUTclose = 0
`

// generateStunnelProxyConfig loads stunnel proxy configuration from app settings
func (t *Task) generateStunnelProxyConfig() (stunnelProxyConfig, error) {
	var proxyConfig stunnelProxyConfig
	tcpProxyString := settings.Settings.DvmOpts.StunnelTCPProxy
	if tcpProxyString != "" {
		t.Log.Info(fmt.Sprintf("Found TCP proxy string [%v]. Configuring Stunnel proxy.",
			tcpProxyString))
		url, err := url.Parse(tcpProxyString)
		if err != nil {
			t.Log.Error(err, fmt.Sprintf("failed to parse %s setting", settings.TCPProxyKey))
			return proxyConfig, liberr.Wrap(err)
		}
		proxyConfig.ProxyHost = url.Host
		if url.User != nil {
			proxyConfig.ProxyUsername = url.User.Username()
			if pass, set := url.User.Password(); set {
				proxyConfig.ProxyPassword = pass
			}
		}
	}
	return proxyConfig, nil
}

func (t *Task) createStunnelConfig() error {
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

	// Generate stunnel certs
	err = t.setupCerts()
	if err != nil {
		return err
	}

	srcStunnelProxyConfig, err := t.generateStunnelProxyConfig()
	if err != nil {
		return err
	}

	// openssl library? to generate new certs

	// Create same stunnel configmap with certs on both source+destination
	// https://github.com/konveyor/pvc-migrate/blob/master/3_run_rsync/templates/stunnel.yml.j2

	// Stunnel configmap consumption can follow 2 approaches:
	// On destination stunnel is sidecar container for rsync, stunnel pod is
	// exposed via route On source, can do sidecar approach; can also do
	// deployment of stunnel and use it for all migrations

	// For source stunnel pod, must mount certs into /etc/stunnel (see
	// https://github.com/konveyor/pvc-migrate/blob/master/3_run_rsync/tasks/rsync.yml#L54)
	// and write stunnel conf to /etc/stunnel/stunnel.conf

	// For source configmap: see https://github.com/konveyor/pvc-migrate/blob/master/3_run_rsync/tasks/rsync.yml#L47
	// For destination configmap: see https://github.com/konveyor/pvc-migrate/blob/master/3_run_rsync/templates/stunnel.yml.j2#L10

	// Create 1 rsync client pod per PVC
	// Create 1 stunnel pod per namespace

	// SOURCE
	// Create 1 rsync client pod per PVC and 1 stunnel pod per namespace
	// Create 1 stunnel svc - rsync client talks to stunnel svc

	// DESTINATION
	// Create 1 rsync transfer+stunnel pod per namespace
	// Create 1 stunnel svc
	pvcMap := t.getPVCNamespaceMap()

	for ns, _ := range pvcMap {
		// Declare config
		rsyncRoute, err := t.getRsyncRoute(ns)
		if err != nil {
			return err
		}
		srcStunnelConf := stunnelConfig{
			Namespace:          ns,
			StunnelPort:        2222,
			RsyncPort:          22,
			RsyncRoute:         rsyncRoute,
			stunnelProxyConfig: srcStunnelProxyConfig,
			VerifyCA:           settings.Settings.StunnelVerifyCA,
			VerifyCALevel:      settings.Settings.StunnelVerifyCALevel,
		}

		destStunnelConf := stunnelConfig{
			Namespace:   ns,
			StunnelPort: 2222,
			RsyncPort:   22,
			RsyncRoute:  rsyncRoute,
		}

		// Generate templates
		var clientTpl bytes.Buffer
		var destTpl bytes.Buffer
		clientTemp, err := template.New("config").Parse(stunnelClientConfigTemplate)
		if err != nil {
			return err
		}
		destTemp, err := template.New("config").Parse(stunnelDestinationConfigTemplate)
		if err != nil {
			return err
		}

		// Execute templates
		err = clientTemp.Execute(&clientTpl, srcStunnelConf)
		if err != nil {
			return err
		}
		err = destTemp.Execute(&destTpl, destStunnelConf)
		if err != nil {
			return err
		}

		// Generate configmaps
		clientConfigMap := corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      DirectVolumeMigrationStunnelConfig,
				Labels: map[string]string{
					"app": DirectVolumeMigrationRsyncTransfer,
				},
			},
		}
		err = yaml.Unmarshal(clientTpl.Bytes(), &clientConfigMap)
		if err != nil {
			return err
		}

		destConfigMap := corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      DirectVolumeMigrationStunnelConfig,
				Labels: map[string]string{
					"app": DirectVolumeMigrationRsyncTransfer,
				},
			},
		}
		err = yaml.Unmarshal(destTpl.Bytes(), &destConfigMap)
		if err != nil {
			return err
		}

		// Create configmaps on source + dest
		t.Log.Info(fmt.Sprintf("Creating Stunnel client ConfigMap [%v/%v] on source cluster.",
			clientConfigMap.Namespace, clientConfigMap.Name))
		err = srcClient.Create(context.TODO(), &clientConfigMap)
		if k8serror.IsAlreadyExists(err) {
			t.Log.Info("Configmap already exists on destination", "namespace", clientConfigMap.Namespace)
		} else if err != nil {
			return err
		}

		t.Log.Info(fmt.Sprintf("Creating Stunnel client ConfigMap [%v/%v] on destination cluster.",
			clientConfigMap.Namespace, clientConfigMap.Name))
		err = destClient.Create(context.TODO(), &destConfigMap)
		if k8serror.IsAlreadyExists(err) {
			t.Log.Info("Configmap already exists on destination", "namespace", destConfigMap.Namespace)
		} else if err != nil {
			return err
		}
	}
	return nil
}

func (t *Task) setupCerts() error {
	// Get client for source
	srcClient, err := t.getSourceClient()
	if err != nil {
		return err
	}
	// Get client for destination
	destClient, err := t.getDestinationClient()
	if err != nil {
		return err
	}

	// steps
	// 1. Generate CA cert
	// 2. Loop through all namespace generating new certs for each namespace
	// 3. Create secret in src+destination namespaces containing each cert
	// 4. Rsync client+transfer pods mount certs from secret

	// Skip CAbundle generation if configmap already exists
	// TODO: Need to handle case where configmap gets deleted and 2 versions of
	// CA bundle exist
	t.Log.Info("Generating CA Bundle for Stunnel")
	caPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return err
	}

	subj := pkix.Name{
		CommonName:         "openshift.io",
		Country:            []string{"US"},
		Province:           []string{"NC"},
		Locality:           []string{"RDU"},
		Organization:       []string{"Migration Engineering"},
		OrganizationalUnit: []string{"Engineering"},
	}

	certTemp := x509.Certificate{
		SerialNumber:          big.NewInt(2020),
		Subject:               subj,
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	caBytes, err := x509.CreateCertificate(
		rand.Reader,
		&certTemp,
		&certTemp,
		&caPrivKey.PublicKey,
		caPrivKey,
	)
	if err != nil {
		return err
	}

	t.Log.Info("Generating ca.crt/tls.crt for Stunnel")
	caPEM := new(bytes.Buffer)
	err = pem.Encode(caPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	})
	if err != nil {
		return err
	}

	t.Log.Info("Generating tls.key for Stunnel")
	caPrivKeyPEM := new(bytes.Buffer)
	err = pem.Encode(caPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(caPrivKey),
	})
	if err != nil {
		return err
	}

	// Create secret in each namespace  src+dest with tls.crt = caPEM and tls.key
	// = caPrivKeyPEM
	// Secret data contains:
	// ca.crt
	// tls.crt (right now equal to ca.crt)
	// tls.key

	pvcMap := t.getPVCNamespaceMap()
	for ns, _ := range pvcMap {
		srcSecret := corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      DirectVolumeMigrationStunnelCerts,
				Labels: map[string]string{
					"app": DirectVolumeMigrationRsyncTransfer,
				},
			},
			Data: map[string][]byte{
				"tls.crt": caPEM.Bytes(),
				"ca.crt":  caPEM.Bytes(),
				"tls.key": caPrivKeyPEM.Bytes(),
			},
		}
		destSecret := srcSecret
		t.Log.Info(fmt.Sprintf("Creating Stunnel CA Bundle and Cert/Key Secret [%v/%v] on source cluster",
			srcSecret.Namespace, srcSecret.Name))
		err = srcClient.Create(context.TODO(), &srcSecret)
		if k8serror.IsAlreadyExists(err) {
			t.Log.Info("Secret already exists on source", "namespace", srcSecret.Namespace)
		} else if err != nil {
			return err
		}
		t.Log.Info(fmt.Sprintf("Creating Stunnel CA Bundle and Cert/Key Secret [%v/%v] on destination cluster",
			destSecret.Namespace, destSecret.Name))
		err = destClient.Create(context.TODO(), &destSecret)
		if k8serror.IsAlreadyExists(err) {
			t.Log.Info("Secret already exists on destination", "namespace", destSecret.Namespace)
		} else if err != nil {
			return err
		}
	}
	return nil
}

// Create stunnel client pods + svc
func (t *Task) createStunnelClientPods() error {
	srcClient, err := t.getSourceClient()
	if err != nil {
		return err
	}

	// Get transfer image for source cluster
	cluster, err := t.Owner.GetSourceCluster(t.Client)
	if err != nil {
		return err
	}

	t.Log.Info("Getting image for Stunnel client Pods that will be created on source MigCluster")
	transferImage, err := cluster.GetRsyncTransferImage(t.Client)
	if err != nil {
		return err
	}
	t.Log.Info(fmt.Sprintf("Using image [%v] for Stunnel client Pods", transferImage))

	t.Log.Info("Getting limits and requests for Stunnel client Pods")
	limits, requests, err := getPodResourceLists(t.Client, STUNNEL_POD_CPU_LIMIT, STUNNEL_POD_MEMORY_LIMIT, STUNNEL_POD_CPU_REQUEST, STUNNEL_POD_MEMORY_REQUEST)
	if err != nil {
		return err
	}
	pvcMap := t.getPVCNamespaceMap()

	dvmLabels := t.buildDVMLabels()
	dvmLabels["purpose"] = DirectVolumeMigrationStunnel

	isRsyncPrivileged, err := isRsyncPrivileged(srcClient)
	if err != nil {
		return err
	}
	t.Log.Info(fmt.Sprintf("Stunnel client pods will be created with privileged=[%v]",
		isRsyncPrivileged))

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
						Name:       "stunnel",
						Protocol:   corev1.ProtocolTCP,
						Port:       int32(2222),
						TargetPort: intstr.IntOrString{Type: intstr.Int, IntVal: 2222},
					},
				},
				Selector: dvmLabels,
				Type:     corev1.ServiceTypeClusterIP,
			},
		}
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
		}
		trueBool := true
		runAsUser := int64(0)
		containers := []corev1.Container{}

		containers = append(containers, corev1.Container{
			Name:    "stunnel",
			Image:   transferImage,
			Command: []string{"/bin/stunnel", "/etc/stunnel/stunnel.conf"},
			Ports: []corev1.ContainerPort{
				{
					Name:          "stunnel",
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
			Resources: corev1.ResourceRequirements{
				Limits:   limits,
				Requests: requests,
			},
		})

		dvmLabels := t.buildDVMLabels()
		dvmLabels["purpose"] = DirectVolumeMigrationStunnel

		clientPod := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      DirectVolumeMigrationStunnelTransfer,
				Namespace: ns,
				Labels:    dvmLabels,
			},
			Spec: corev1.PodSpec{
				Volumes:    volumes,
				Containers: containers,
			},
		}
		t.Log.Info(fmt.Sprintf("Creating Stunnel Client Service [%v/%v] on source cluster",
			svc.Namespace, svc.Name))
		err := srcClient.Create(context.TODO(), &svc)
		if k8serror.IsAlreadyExists(err) {
			t.Log.Info("Stunnel client svc already exists on source", "namespace", svc.Namespace)
		} else if err != nil {
			return err
		}
		t.Log.Info("stunnel client svc created", "name", clientPod.Name, "namespace", svc.Namespace)
		t.Log.Info(fmt.Sprintf("Creating Stunnel Client Pod [%v/%v] on source cluster",
			svc.Namespace, svc.Name))
		err = srcClient.Create(context.TODO(), &clientPod)
		if k8serror.IsAlreadyExists(err) {
			t.Log.Info("Stunnel client pod already exists on source", "namespace", clientPod.Namespace)
		} else if err != nil {
			return err
		}
		t.Log.Info("stunnel client pod created", "name", clientPod.Name, "namespace", clientPod.Namespace)
	}
	return nil
}

// check if stunnel client pods are running
func (t *Task) areStunnelClientPodsRunning() (bool, error) {
	// Get client for destination
	srcClient, err := t.getSourceClient()
	if err != nil {
		return false, err
	}

	pvcMap := t.getPVCNamespaceMap()

	dvmLabels := t.buildDVMLabels()
	dvmLabels["purpose"] = DirectVolumeMigrationStunnel
	selector := labels.SelectorFromSet(dvmLabels)

	for ns, _ := range pvcMap {
		pods := corev1.PodList{}
		err = srcClient.List(
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
			t.Log.Info(fmt.Sprintf("dvm cr: %s/%s, number of stunnel pods expected %d, found %d", t.Owner.Namespace, t.Owner.Name, 1, len(pods.Items)))
			return false, nil
		}
		for _, pod := range pods.Items {
			if pod.Status.Phase != corev1.PodRunning {
				for _, podCond := range pod.Status.Conditions {
					if podCond.Reason == corev1.PodReasonUnschedulable {
						t.Log.Info(fmt.Sprintf("Found UNSCHEDULABLE Stunnel Client Pod [%v/%v] "+
							"with Phase=[%v] on source cluster. Message: [%v].",
							pod.Namespace, pod.Name, pod.Status.Phase, podCond.Message))
						return false, nil
					}
				}
				t.Log.Info(fmt.Sprintf("Stunnel Client Pod [%v/%v] Phase=[%v] is not yet running on source cluster.",
					pod.Namespace, pod.Name, pod.Status.Phase))
				return false, nil
			}
		}
	}

	return true, nil
}
