package persistent

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	_ "github.com/lib/pq"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/config"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/helper"
)

type PostgresUpsertHandler struct {
	cfg *config.DebeziumConfig
	Db  *sql.DB
}

func NewPostgresUpsertHandler(cfg *config.DebeziumConfig) *PostgresUpsertHandler {
	db, err := sql.Open("postgres", cfg.TargetConfig.PostgresURL)
	if err != nil {
		log.Println("Postgres connection failed:", err)
		return nil
	}

	h := &PostgresUpsertHandler{
		cfg: cfg,
		Db:  db,
	}

	h.migratePostgres()
	return h
}

func (h *PostgresUpsertHandler) Upsert(jsonStr string) error {
	var root map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &root); err != nil {
		log.Println("Postgres upsert failed: unmarshal error", err)
		return err
	}

	payload, ok := root["payload"].(map[string]interface{})
	if !ok {
		return nil
	}

	after, ok := payload["after"].(map[string]interface{})
	if !ok || after == nil {
		return nil
	}

	variable := after["variable"].(string)
	value := after["value"].(string)

	var description *string
	if d, ok := after["description"].(string); ok {
		description = &d
	}

	var version *float64
	if v, ok := after["version"].(float64); ok {
		version = &v
	}

	sqlStmt := `
		INSERT INTO unified_system_parameter (variable, value, description, version)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (variable) DO UPDATE SET value = EXCLUDED.value, description = EXCLUDED.description, version = EXCLUDED.version
	`

	if _, err := h.Db.Exec(sqlStmt, variable, value, description, version); err != nil {
		helper.SendMetric(helper.TagBuilder{
			EventType: "postgres_upsert_failed",
			Variable:  variable,
			Version:   fmt.Sprintf("%.0f", *version),
		})
		return err
	}

	helper.SendMetric(helper.TagBuilder{
		EventType: "postgres_upsert_success",
		Variable:  variable,
		Version:   fmt.Sprintf("%.0f", *version),
	})

	return nil
}

func (h *PostgresUpsertHandler) migratePostgres() {
	sqlStmt := `
	CREATE TABLE IF NOT EXISTS unified_system_parameter (
		variable VARCHAR(255) PRIMARY KEY,
		value TEXT NOT NULL,
		description TEXT,
		version FLOAT
	);

	-- function to create trigger NOTIFY
	CREATE OR REPLACE FUNCTION notify_unified_system_parameter()
	RETURNS trigger AS $$
	BEGIN
		PERFORM pg_notify(
			'unified_system_parameter_channel',
			NEW.variable
		);
		RETURN NEW;
	END;
	$$ LANGUAGE plpgsql;

	-- trigger after insert or update
	DO $$
	BEGIN
		IF NOT EXISTS (
			SELECT 1 FROM pg_trigger WHERE tgname = 'unified_system_parameter_trigger'
		) THEN
			CREATE TRIGGER unified_system_parameter_trigger
			AFTER INSERT OR UPDATE ON unified_system_parameter
			FOR EACH ROW
			EXECUTE FUNCTION notify_unified_system_parameter();
		END IF;
	END;
	$$;
	`

	if _, err := h.Db.Exec(sqlStmt); err != nil {
		log.Println("Postgres migration failed:", err)
		return
	}

	fmt.Println("Postgres migration done.")
}
