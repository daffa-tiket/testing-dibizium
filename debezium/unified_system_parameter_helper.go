package debezium

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/config"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/helper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type UnifiedSystemParameterHelper struct {
	cfg            *config.DebeziumConfig
	mongoClient    *mongo.Client
	pg             *sql.DB
	watcherRunning bool
}

type USP interface {
    GetValue(variable string) json.RawMessage
    GetValueAs(variable string, out any) error
    GetDescription(variable string) string
}

var cache sync.Map

var instance *UnifiedSystemParameterHelper

func NewUnifiedSystemParameterHelper(cfg *config.DebeziumConfig, mongoClient *mongo.Client, pg *sql.DB) *UnifiedSystemParameterHelper {

	helper := &UnifiedSystemParameterHelper{
		cfg:         cfg,
		mongoClient: mongoClient,
		pg:          pg,
	}

	helper.StartScheduledRefresh()

	instance = helper
	return helper
}

func GetUnifiedSystemParameterHelper() USP {
	return instance	
}

//////////////////////////////////////////////////////
//                  CACHE REFRESH
//////////////////////////////////////////////////////

func (h *UnifiedSystemParameterHelper) refreshCacheFromDb(all []Parameter) {
	for _, p := range all {
		cache.Store(p.Variable, p)
	}
}

//////////////////////////////////////////////////////
//                  SCHEDULE
//////////////////////////////////////////////////////

func (h *UnifiedSystemParameterHelper) StartScheduledRefresh() {
	if !h.cfg.WorkerConfig.EnableLocalMemory {
		return
	}

	// Layer 1: watcher (optional)
	if h.cfg.WorkerConfig.EnableRealtimeListener {
		switch h.cfg.TargetDB {
		case "MONGO":
			h.startMongoWatcher()
		case "POSTGRES":
			h.startPostgresListener()
		}
	}

	// Layer 2: fallback polling
	go func() {
		ticker := time.NewTicker(time.Duration(h.cfg.WorkerConfig.PeriodSec) * time.Second)
		defer ticker.Stop()

		h.doQueryDb()

		time.Sleep(time.Duration(h.cfg.WorkerConfig.InitialDelaySec) * time.Second)

		for range ticker.C {
			h.doQueryDb()
		}
	}()
}

func (h *UnifiedSystemParameterHelper) doQueryDb() {
	all, err := h.dbQueryAllParameters()
	if err == nil {
		h.refreshCacheFromDb(all)
		helper.SendMetric(helper.TagBuilder{
			EventType: "unified_system_parameter_cache_refreshed",
		})
	}
}

//////////////////////////////////////////////////////
//                MONGO WATCHER
//////////////////////////////////////////////////////

func (h *UnifiedSystemParameterHelper) startMongoWatcher() {
	if h.watcherRunning {
		return
	}
	h.watcherRunning = true

	go func() {
		defer func() { h.watcherRunning = false }()

		collection := h.mongoClient.
			Database(h.cfg.TargetConfig.MongoDB).
			Collection(h.cfg.TargetConfig.MongoCol)

		ctx := context.Background()
		stream, err := collection.Watch(ctx, mongo.Pipeline{})
		if err != nil {
			log.Println("mongo watcher error:", err)
			return
		}
		defer stream.Close(ctx)

		helper.SendMetric(helper.TagBuilder{
			EventType: "unified_system_parameter_mongo_watcher_started",
		})

		for stream.Next(ctx) {
			var ev struct {
				FullDocument struct {
					Variable    string  `bson:"variable"`
					ValueStr    string  `bson:"value"`
					Description string  `bson:"description"`
					Version     float64 `bson:"version"`
				} `bson:"fullDocument"`
			}

			if err := stream.Decode(&ev); err != nil {
				log.Println("decode err:", err)
				continue
			}

			value := json.RawMessage(ev.FullDocument.ValueStr)

			p := Parameter{
				Variable:    ev.FullDocument.Variable,
				Value:       value,
				Description: ev.FullDocument.Description,
				Version:     ev.FullDocument.Version,
			}

			cache.Store(p.Variable, p)
			helper.SendMetric(helper.TagBuilder{
				EventType: "unified_system_parameter_mongo_watcher_update",
				Variable:  p.Variable,
				Version:   fmt.Sprintf("%.0f", p.Version),
			})
		}
	}()
}

//////////////////////////////////////////////////////
//              POSTGRES LISTEN/NOTIFY
//////////////////////////////////////////////////////

func (h *UnifiedSystemParameterHelper) startPostgresListener() {
	if h.watcherRunning {
		return
	}
	h.watcherRunning = true

	go func() {
		defer func() { h.watcherRunning = false }()

		listener := pq.NewListener(
			h.cfg.TargetConfig.PostgresURL,
			10*time.Second,
			1*time.Minute,
			func(ev pq.ListenerEventType, err error) {
				if err != nil {
					log.Println("listener err:", err)
				}
			},
		)

		err := listener.Listen("unified_system_parameter_channel")
		if err != nil {
			log.Println("listen err:", err)
			return
		}

		helper.SendMetric(helper.TagBuilder{
			EventType: "unified_system_parameter_listener_started",
		})

		for h.watcherRunning {
			select {
			case n := <-listener.Notify:
				if n == nil {
					continue
				}

				variable := n.Extra
				p, err := h.dbQueryParameterPostgres(variable)
				if err == nil && p != nil {
					cache.Store(variable, *p)
					helper.SendMetric(helper.TagBuilder{
						EventType: "unified_system_parameter_listener_update",
						Variable:  variable,
						Version:   fmt.Sprintf("%.0f", p.Version),
					})
				}

			case <-time.After(30 * time.Second):
				go listener.Ping()
			}
		}
	}()
}

//////////////////////////////////////////////////////
//            QUERY SINGLE (POSTGRES)
//////////////////////////////////////////////////////

func (h *UnifiedSystemParameterHelper) dbQueryParameterPostgres(variable string) (*Parameter, error) {
	row := h.pg.QueryRow(`
		SELECT variable, value, description, version 
		FROM unified_system_parameter 
		WHERE variable = $1
	`, variable)

	var p Parameter
	var valueStr string

	if err := row.Scan(&p.Variable, &valueStr, &p.Description, &p.Version); err != nil {
		return nil, err
	}

	p.Value = json.RawMessage(valueStr)
	return &p, nil
}

//////////////////////////////////////////////////////
//            QUERY ALL (POSTGRES / MONGO)
//////////////////////////////////////////////////////

func (h *UnifiedSystemParameterHelper) dbQueryAllParameters() ([]Parameter, error) {
	if h.cfg.TargetDB == "POSTGRES" {
		return h.dbQueryAllParametersPostgres()
	}
	return h.dbQueryAllParametersMongo()
}

func (h *UnifiedSystemParameterHelper) dbQueryAllParametersPostgres() ([]Parameter, error) {
	rows, err := h.pg.Query(`
		SELECT variable, value, description, version 
		FROM unified_system_parameter
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []Parameter

	for rows.Next() {
		var p Parameter
		var valueStr string

		if err := rows.Scan(&p.Variable, &valueStr, &p.Description, &p.Version); err != nil {
			continue
		}

		p.Value = json.RawMessage(valueStr)
		list = append(list, p)
	}

	return list, nil
}

func (h *UnifiedSystemParameterHelper) dbQueryAllParametersMongo() ([]Parameter, error) {
	collection := h.mongoClient.
		Database(h.cfg.TargetConfig.MongoDB).
		Collection(h.cfg.TargetConfig.MongoCol)

	cursor, err := collection.Find(context.Background(), bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())

	var list []Parameter

	for cursor.Next(context.Background()) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}

		p := Parameter{
			Variable:    doc["variable"].(string),
			Value:       json.RawMessage(doc["value"].(string)),
			Description: doc["description"].(string),
			Version:     doc["version"].(float64),
		}

		list = append(list, p)
	}

	return list, nil
}

//////////////////////////////////////////////////////
//            PUBLIC GETTERS
//////////////////////////////////////////////////////

func (h *UnifiedSystemParameterHelper) GetValue(variable string) json.RawMessage {
	val, ok := cache.Load(variable)
	if !ok {
		return nil
	}
	return val.(Parameter).Value
}

func (h *UnifiedSystemParameterHelper) GetValueAs(variable string, out any) error {
	val := h.GetValue(variable)
	if val == nil {
		return fmt.Errorf("not found")
	}
	return json.Unmarshal(val, out)
}

func (h *UnifiedSystemParameterHelper) GetDescription(variable string) string {
	val, ok := cache.Load(variable)
	if !ok {
		return ""
	}
	return val.(Parameter).Description
}
