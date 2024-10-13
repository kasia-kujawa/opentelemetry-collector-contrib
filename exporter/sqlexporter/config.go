// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sqlexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sqlexporter"

import (
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config defines configuration for Syslog exporter.
type Config struct {
	QueueSettings             exporterhelper.QueueConfig `mapstructure:"sending_queue"`
	configretry.BackOffConfig `mapstructure:"retry_on_failure"`
	TimeoutSettings           exporterhelper.TimeoutConfig `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct
}
