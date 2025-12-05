package debezium

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/config"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/connector"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/helper"
)

var once sync.Once
var DebeziumCfg *config.DebeziumConfig

func Initialize() {
	once.Do(func() {
		// Load config from .env
		var err error
		DebeziumCfg, err = config.LoadDebeziumConfig(".env")
		if err != nil {
			log.Printf("Failed to load Debezium config: %v", err)
			return
		}

		if !DebeziumCfg.Enabled {
			fmt.Println("Debezium is disabled in the configuration.")
			return
		}

		helper.CreateNewMetric(*DebeziumCfg)

		connectorManager := connector.NewDebeziumConnectorManager(DebeziumCfg)
		if err := connectorManager.Start(); err != nil {
			log.Printf("Failed to start Debezium Connector Manager: %v", err)
			return
		}

		NewUnifiedSystemParameterHelper(DebeziumCfg, connectorManager.Client, connectorManager.Db)

		fmt.Println("Debezium initialized with mode:", strings.ToUpper(DebeziumCfg.Mode))

	})
}
