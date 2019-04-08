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

package util

import (
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// BuildRestConfig creates an insecure REST config from a clusterURL and bearerToken
// TODO: add support for creating a secure rest.Config
func BuildRestConfig(clusterURL string, bearerToken string) *rest.Config {
	clusterConfig := &rest.Config{
		Host:        clusterURL,
		BearerToken: bearerToken,
	}
	clusterConfig.Insecure = true
	return clusterConfig
}

// BuildControllerRuntimeClient builds a controller-runtime client for interacting with
// a K8s cluster.
func BuildControllerRuntimeClient(config *rest.Config) (c client.Client, err error) {
	c, err = client.New(config, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		return nil, err
	}
	return c, nil
}
