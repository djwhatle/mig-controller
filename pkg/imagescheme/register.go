package imagescheme

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/openshift/api/image/docker10"
	"github.com/openshift/api/image/dockerpre012"
	imagev1API "github.com/openshift/api/image/v1"
)

var (
	GroupName     = "image.openshift.io"
	GroupVersion  = schema.GroupVersion{Group: GroupName, Version: "v1"}
	schemeBuilder = runtime.NewSchemeBuilder(addKnownTypes, docker10.AddToScheme, dockerpre012.AddToScheme, corev1.AddToScheme)
	// Install is a function which adds this version to a scheme
	Install = schemeBuilder.AddToScheme

	// SchemeGroupVersion generated code relies on this name
	// Deprecated
	SchemeGroupVersion = GroupVersion
	// AddToScheme exists solely to keep the old generators creating valid code
	// DEPRECATED
	AddToScheme = schemeBuilder.AddToScheme
)

// Resource generated code relies on this being here, but it logically belongs to the group
// DEPRECATED
func Resource(resource string) schema.GroupResource {
	return schema.GroupResource{Group: GroupName, Resource: resource}
}

// Adds the list of known types to api.Scheme.
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion,
		&imagev1API.Image{},
		&imagev1API.ImageList{},
		&imagev1API.ImageSignature{},
		&imagev1API.ImageStream{},
		&imagev1API.ImageStreamList{},
		&imagev1API.ImageStreamMapping{},
		&imagev1API.ImageStreamTag{},
		&imagev1API.ImageStreamTagList{},
		&imagev1API.ImageStreamImage{},
		&imagev1API.ImageStreamLayers{},
		&imagev1API.ImageStreamImport{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
