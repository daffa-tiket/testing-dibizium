package helper

import (
	"os"

	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/config"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/metrics"
)


type TagBuilder struct {
	EventType string
	Variable string
	Version string
}

var entity string = "debezium-cdc-event"

var metricInstace metrics.MonitorStatsd

func CreateNewMetric(config config.DebeziumConfig) {

	if !config.MetricConfig.Enabled {
		return
	}

	monitor, err := metrics.NewMonitor(config.MetricConfig.StatsDHost, config.MetricConfig.StatsDPort, config.MetricConfig.ServiceName)
	if err != nil {
		return
	}

	metricInstace = monitor
}

func SendMetric(tagBuilder TagBuilder) {
	tags := tagsBuilder(tagBuilder)
	metricInstace.CustomMonitorCounter(entity, metrics.EVENTS, metrics.Success, 0, tags)
}

func tagsBuilder(tagBuilder TagBuilder) map[string]interface{} {
	tags := make(map[string]interface{})
	host := "unknown"
	if os.Getenv("HOSTNAME") != "" {
		host = os.Getenv("HOSTNAME")
	}
	tags["ORIGIN"] = host
	tags["STATUS"] = tagBuilder.EventType
	tags["TYPE"] = tagBuilder.Variable
	tags["KEY_TYPE"] = tagBuilder.Version

	return tags
}