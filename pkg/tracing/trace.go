package tracing

import (
	"fmt"
	"io"
	"sync"

	opentracing "github.com/opentracing/opentracing-go"
	jaeger "github.com/uber/jaeger-client-go"
	config "github.com/uber/jaeger-client-go/config"
)

var msmInstance MigrationSpanMap
var createMsmMapOnce sync.Once

// MigrationSpanMap provides a map between MigMigration UID and associated Jaeger span.
// This is required so that all controllers can attach child spans to the correct
// parent migmigration span for unified tracing of work done during migrations.
type MigrationSpanMap struct {
	mutex              sync.RWMutex
	migrationUIDToSpan map[string]opentracing.Span
}

// InitJaeger returns an instance of Jaeger Tracer that samples 100% of traces and logs all spans to stdout.
func InitJaeger(service string) (opentracing.Tracer, io.Closer) {
	cfg := &config.Configuration{
		ServiceName: service,
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		Reporter: &config.ReporterConfig{
			LogSpans: true,
		},
	}
	tracer, closer, err := cfg.NewTracer(config.Logger(jaeger.StdLogger))
	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
	}
	return tracer, closer
}

// SetSpanForMigrationUID sets the parent jaeger span for a migration
func SetSpanForMigrationUID(migrationUID string, span opentracing.Span) {
	// Init map if needed
	createMsmMapOnce.Do(func() {
		msmInstance = MigrationSpanMap{}
		msmInstance.migrationUIDToSpan = make(map[string]opentracing.Span)
	})
	msmInstance.mutex.RLock()
	defer msmInstance.mutex.RUnlock()

	msmInstance.migrationUIDToSpan[migrationUID] = span
	return
}

// GetSpanForMigrationUID returns the parent jaeger span for a migration
func GetSpanForMigrationUID(migrationUID string) opentracing.Span {
	// Init map if needed
	createMsmMapOnce.Do(func() {
		msmInstance = MigrationSpanMap{}
		msmInstance.migrationUIDToSpan = make(map[string]opentracing.Span)
	})
	msmInstance.mutex.RLock()
	defer msmInstance.mutex.RUnlock()

	migrationSpan, ok := msmInstance.migrationUIDToSpan[migrationUID]
	if !ok {
		return nil
	}
	return migrationSpan
}