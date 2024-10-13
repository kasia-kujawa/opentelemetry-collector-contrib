// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sqlexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sqlexporter"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sqlexporter/internal/metadata"
)

// NewFactory returns a new factory for the syslog exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithMetrics(createMetricsExporter, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	qs := exporterhelper.NewDefaultQueueConfig()
	qs.Enabled = false

	return &Config{
		BackOffConfig:   configretry.NewDefaultBackOffConfig(),
		QueueSettings:   qs,
		TimeoutSettings: exporterhelper.NewDefaultTimeoutConfig(),
	}
}

func createMetricsExporter(
	ctx context.Context,
	params exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	exp, err := newMetricsExporter(ctx, params, cfg.(*Config))
	if err != nil {
		return nil, fmt.Errorf("failed to create the metrics exporter: %w", err)
	}

	return exp, nil
}
