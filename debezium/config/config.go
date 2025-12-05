package config

import (
	"os"
	"strconv"
	"strings"
	"github.com/joho/godotenv"
)

type KafkaConfig struct {
	ConnectURL    string
	Topic         string
	Bootstrap     string
	GroupID       string
	PollIntervalMs int
}

type PostgresSourceConfig struct {
	Host       string
	Port       int
	User       string
	Password   string
	DBName     string
	TableIncludeList []string
	SlotName   string
}

type TargetConfig struct {
	MongoURI      string
	MongoDB       string
	MongoCol      string
	PostgresURL   string
	PostgresUser  string
	PostgresPass  string
	PostgresTable string
}

type WorkerConfig struct {
	EnableLocalMemory       bool
	InitialDelaySec         int
	PeriodSec               int
	EnableRealtimeListener  bool
}

type MetricConfig struct {
	Enabled       bool
	StatsDHost   string
	StatsDPort   string
	ServiceName  string
}

type DebeziumConfig struct {
	Enabled          bool
	Mode             string
	TargetDB 	     string
	ServerName       string
	UseSinkConnector bool

	KafkaConfig      KafkaConfig
	SourceConfig     PostgresSourceConfig
	TargetConfig     TargetConfig
	WorkerConfig     WorkerConfig

	MetricConfig     MetricConfig
}

func LoadDebeziumConfig(envPath string) (*DebeziumConfig, error) {
	_ = godotenv.Load(envPath)

	enabled := strings.ToLower(os.Getenv("CDC_DEBEZIUM_ENABLED")) == "true"
	useSink := strings.ToLower(os.Getenv("CDC_DEBEZIUM_USE_SINK_CONNECTOR")) == "true"
	kafkaPoll, _ := strconv.Atoi(os.Getenv("CDC_DEBEZIUM_KAFKA_POLL_INTERVAL_MS"))
	sourcePort, _ := strconv.Atoi(os.Getenv("CDC_DEBEZIUM_SOURCE_POSTGRES_PORT"))
	workerDelay, _ := strconv.Atoi(os.Getenv("CDC_DEBEZIUM_WORKER_INITIAL_DELAY_SEC"))
	workerPeriod, _ := strconv.Atoi(os.Getenv("CDC_DEBEZIUM_WORKER_PERIOD_SEC"))
	workerMemory := strings.ToLower(os.Getenv("CDC_DEBEZIUM_WORKER_ENABLE_LOCAL_MEMORY")) == "true"
	workerRealtime := strings.ToLower(os.Getenv("CDC_DEBEZIUM_WORKER_ENABLE_REALTIME_LISTENER")) == "true"

	return &DebeziumConfig{
		Enabled:          enabled,
		Mode:             os.Getenv("CDC_DEBEZIUM_MODE"),
		TargetDB: 	  os.Getenv("CDC_DEBEZIUM_TARGET_DB"),
		ServerName:       os.Getenv("CDC_DEBEZIUM_SERVER_NAME"),
		UseSinkConnector: useSink,
		KafkaConfig: KafkaConfig{
			ConnectURL:    os.Getenv("CDC_DEBEZIUM_KAFKA_CONNECT_URL"),
			Topic:         os.Getenv("CDC_DEBEZIUM_KAFKA_TOPIC"),
			Bootstrap:     os.Getenv("CDC_DEBEZIUM_KAFKA_BOOTSTRAP_SERVERS"),
			GroupID:       os.Getenv("CDC_DEBEZIUM_KAFKA_GROUP_ID"),
			PollIntervalMs: kafkaPoll,
		},
		SourceConfig: PostgresSourceConfig{
			Host:       os.Getenv("CDC_DEBEZIUM_SOURCE_POSTGRES_HOST"),
			Port:       sourcePort,
			User:       os.Getenv("CDC_DEBEZIUM_SOURCE_POSTGRES_USER"),
			Password:   os.Getenv("CDC_DEBEZIUM_SOURCE_POSTGRES_PASSWORD"),
			DBName:     os.Getenv("CDC_DEBEZIUM_SOURCE_POSTGRES_DB_NAME"),
			TableIncludeList: strings.Split(os.Getenv("CDC_DEBEZIUM_SOURCE_POSTGRES_TABLE_INCLUDE_LIST"), ","),
			SlotName:   os.Getenv("CDC_DEBEZIUM_SOURCE_POSTGRES_SLOT_NAME"),
		},
		TargetConfig: TargetConfig{
			MongoURI:      os.Getenv("CDC_DEBEZIUM_TARGET_MONGO_URI"),
			MongoDB:       os.Getenv("CDC_DEBEZIUM_TARGET_MONGO_DATABASE"),
			MongoCol:      os.Getenv("CDC_DEBEZIUM_TARGET_MONGO_COLLECTION"),
			PostgresURL:   os.Getenv("CDC_DEBEZIUM_TARGET_POSTGRES_URL"),
			PostgresUser:  os.Getenv("CDC_DEBEZIUM_TARGET_POSTGRES_USER"),
			PostgresPass:  os.Getenv("CDC_DEBEZIUM_TARGET_POSTGRES_PASSWORD"),
			PostgresTable: os.Getenv("CDC_DEBEZIUM_TARGET_POSTGRES_TABLE"),
		},
		WorkerConfig: WorkerConfig{
			EnableLocalMemory:      workerMemory,
			InitialDelaySec:        workerDelay,
			PeriodSec:              workerPeriod,
			EnableRealtimeListener: workerRealtime,
		},
		MetricConfig: MetricConfig{
			Enabled:      strings.ToLower(os.Getenv("CDC_DEBEZIUM_METRIC_ENABLED")) == "true",
			StatsDHost:   os.Getenv("CDC_DEBEZIUM_METRIC_STATSD_HOST"),
			StatsDPort:   os.Getenv("CDC_DEBEZIUM_METRIC_STATSD_PORT"),
			ServiceName:  os.Getenv("CDC_DEBEZIUM_METRIC_SERVICE_NAME"),
		},
	}, nil
}
