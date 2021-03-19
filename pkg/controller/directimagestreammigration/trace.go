/*
Copyright 2021 Red Hat Inc.

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

package directimagestreammigration

import (
	liberr "github.com/konveyor/controller/pkg/error"
	migapi "github.com/konveyor/mig-controller/pkg/apis/migration/v1alpha1"
	"github.com/konveyor/mig-controller/pkg/errorutil"
	"github.com/konveyor/mig-controller/pkg/settings"
	migtrace "github.com/konveyor/mig-controller/pkg/tracing"
	"github.com/opentracing/opentracing-go"
	"k8s.io/apimachinery/pkg/api/errors"
)

func (r *ReconcileDirectImageStreamMigration) initTracer(dism migapi.DirectImageStreamMigration) (opentracing.Span, error) {
	// Exit if tracing disabled
	if !settings.Settings.JaegerOpts.Enabled {
		return nil, nil
	}

	// Set tracer on reconciler if it's not already present.
	// We will never close this, so the 'closer' is discarded.
	if r.tracer == nil {
		r.tracer, _ = migtrace.InitJaeger("DirectImageStreamMigration")
	}

	// Go from dism -> dim -> migration, use migration UID to get span
	dim, err := dism.GetDIMforDISM(r)
	if err != nil {
		if errors.IsNotFound(errorutil.Unwrap(err)) {
			return nil, nil
		}
		return nil, liberr.Wrap(err)
	}
	if dim == nil {
		return nil, nil
	}
	// dim -> migration
	migration, err := dim.GetMigrationForDIM(r)
	if err != nil {
		if errors.IsNotFound(errorutil.Unwrap(err)) {
			return nil, nil
		}
		return nil, liberr.Wrap(err)
	}
	if migration == nil {
		return nil, nil
	}

	// Get overall migration span
	migrationUID := string(migration.GetUID())
	migrationSpan := migtrace.GetSpanForMigrationUID(migrationUID)
	if migrationSpan == nil {
		return nil, nil
	}

	// Get span for current reconcile
	var reconcileSpan opentracing.Span
	if migrationSpan != nil {
		reconcileSpan = r.tracer.StartSpan(
			"dism-reconcile-"+dism.Name, opentracing.ChildOf(migrationSpan.Context()),
		)
	}

	return reconcileSpan, nil
}
