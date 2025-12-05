package debezium

import "encoding/json"

type Parameter struct {
	Variable string
	Value   json.RawMessage
	Description string
	Version float64
}