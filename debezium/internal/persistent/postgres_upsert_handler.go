package persistent

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/internal/config"
	_ "github.com/lib/pq"
)

type PostgresUpsertHandler struct {
	cfg *config.DebeziumConfig
}

func NewPostgresUpsertHandler(cfg *config.DebeziumConfig) *PostgresUpsertHandler {
	h := &PostgresUpsertHandler{cfg: cfg}
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

	db, err := sql.Open("postgres", h.cfg.TargetConfig.PostgresURL)
	if err != nil {
		log.Println("Postgres connection failed:", err)
		return err
	}
	defer db.Close()

	sqlStmt := `
		INSERT INTO unified_system_parameter (variable, value, description)
		VALUES ($1, $2, $3)
		ON CONFLICT (variable) DO UPDATE SET value = EXCLUDED.value, description = EXCLUDED.description
	`
	_, err = db.Exec(sqlStmt, variable, value, description)
	if err != nil {
		log.Println("Postgres upsert failed:", err)
		return err
	}

	fmt.Println("postgres_upsert_success")
	return nil
}

func (h *PostgresUpsertHandler) migratePostgres() {
	db, err := sql.Open("postgres", h.cfg.TargetConfig.PostgresURL)
	if err != nil {
		log.Println("Postgres migrate failed:", err)
		return
	}
	defer db.Close()

	sqlStmt := `
	CREATE TABLE IF NOT EXISTS unified_system_parameter (
		variable VARCHAR(255) PRIMARY KEY,
		value TEXT NOT NULL,
		description TEXT
	);
	`
	if _, err := db.Exec(sqlStmt); err != nil {
		log.Println("Postgres migration failed:", err)
		return
	}
	fmt.Println("Postgres migration done.")
}
