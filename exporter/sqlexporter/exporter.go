// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sqlexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sqlexporter"

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type sqlexporter struct {
	config *Config
	logger *zap.Logger
}

func newMetricsExporter(
	ctx context.Context,
	params exporter.Settings,
	cfg *Config,
) (exporter.Metrics, error) {

	sqlexporter := &sqlexporter{}
	return exporterhelper.NewMetricsExporter(
		ctx,
		params,
		cfg,
		sqlexporter.pushMetricsData,
		exporterhelper.WithTimeout(cfg.TimeoutSettings),
		exporterhelper.WithRetry(cfg.BackOffConfig),
		exporterhelper.WithQueue(cfg.QueueSettings),
	)

}

func valueToString(v pcommon.Value) string {
	return fmt.Sprintf("%s(%s)", v.Type().String(), v.AsString())
}

func logAttributes(header string, m pcommon.Map) {
	if m.Len() == 0 {
		return
	}

	fmt.Printf("%s: \n", header)
	attrPrefix := "     ->"

	// Add offset to attributes if needed.
	headerParts := strings.Split(header, "->")
	if len(headerParts) > 1 {
		attrPrefix = headerParts[0] + attrPrefix
	}

	m.Range(func(k string, v pcommon.Value) bool {
		fmt.Printf("%s %s: %s \n", attrPrefix, k, valueToString(v))
		return true
	})
}

func logInstrumentationScope(il pcommon.InstrumentationScope) {
	fmt.Printf(
		"InstrumentationScope %s %s \n",
		il.Name(),
		il.Version())
	logAttributes("InstrumentationScope attributes", il.Attributes())
}

func logMetricDescriptor(md pmetric.Metric) {
	fmt.Printf("Descriptor: \n")
	fmt.Printf("     -> Name: %s \n", md.Name())
	fmt.Printf("     -> Description: %s \n", md.Description())
	fmt.Printf("     -> Unit: %s \n", md.Unit())
	fmt.Printf("     -> DataType: %s \n", md.Type().String())
}

func logNumberDataPoints(ps pmetric.NumberDataPointSlice) {
	for i := 0; i < ps.Len(); i++ {
		p := ps.At(i)
		fmt.Printf("NumberDataPoints #%d \n", i)
		logAttributes("Data point attributes", p.Attributes())

		fmt.Printf("StartTimestamp: %s \n", p.StartTimestamp())
		fmt.Printf("Timestamp: %s \n", p.Timestamp())
		switch p.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			fmt.Printf("Value: %d \n", p.IntValue())
		case pmetric.NumberDataPointValueTypeDouble:
			fmt.Printf("Value: %f \n", p.DoubleValue())
		}
	}
}

func logMetricDataPoints(m pmetric.Metric) {
	switch m.Type() {
	case pmetric.MetricTypeEmpty:
		return
	case pmetric.MetricTypeGauge:
		logNumberDataPoints(m.Gauge().DataPoints())
	case pmetric.MetricTypeSum:
		data := m.Sum()
		fmt.Printf("     -> IsMonotonic: %t \n", data.IsMonotonic())
		fmt.Printf("     -> AggregationTemporality: %s \n", data.AggregationTemporality().String())
		logNumberDataPoints(data.DataPoints())
	default:
		// this should support all data types see: https://github.com/open-telemetry/opentelemetry-collector/blob/76464a302a0aae71b998b5f4479a978c3b1f3dae/exporter/debugexporter/internal/otlptext/databuffer.go#L89
		fmt.Println("Unknown metric type")
	}
}

// similar to https://github.com/open-telemetry/opentelemetry-collector/blob/76464a302a0aae71b998b5f4479a978c3b1f3dae/exporter/debugexporter/internal/otlptext/metrics.go#L16
func (s *sqlexporter) pushMetricsData(ctx context.Context, md pmetric.Metrics) error {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		fmt.Printf("ResourceMetrics #%d \n", i)
		rm := rms.At(i)
		fmt.Printf("Resource SchemaURL: %s\n", rm.SchemaUrl())
		logAttributes("Resource attributes ", rm.Resource().Attributes())
		ilms := rm.ScopeMetrics()
		for j := 0; j < ilms.Len(); j++ {
			fmt.Printf("ScopeMetrics #%d \n", j)
			ilm := ilms.At(j)
			fmt.Printf("ScopeMetrics SchemaURL: %s \n", ilm.SchemaUrl())
			logInstrumentationScope(ilm.Scope())
			metrics := ilm.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				fmt.Printf("Metric #%d \n", k)
				metric := metrics.At(k)
				logMetricDescriptor(metric)
				logMetricDataPoints(metric)
			}
		}
	}
	return nil
}
