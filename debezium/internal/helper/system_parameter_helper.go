package helper

import (
	"encoding/json"
	"os"
	"sync"
)

type SystemParameterHelper struct {
	cache map[string]interface{}
	mu    sync.RWMutex
}

func NewSystemParameterHelper() *SystemParameterHelper {
	return &SystemParameterHelper{
		cache: make(map[string]interface{}),
	}
}

// GetString returns string param from file/env or default
func (h *SystemParameterHelper) GetString(key, defaultVal string) string {
	h.mu.RLock()
	val, ok := h.cache[key]
	h.mu.RUnlock()

	if ok {
		if s, ok := val.(string); ok {
			return s
		}
	}

	// fallback: env var
	env := os.Getenv(key)
	if env != "" {
		h.mu.Lock()
		h.cache[key] = env
		h.mu.Unlock()
		return env
	}

	return defaultVal
}

// GetJSON returns JSON object from cached value or fallback file
func (h *SystemParameterHelper) GetJSON(key string, defaultFile string) map[string]interface{} {
	h.mu.RLock()
	val, ok := h.cache[key]
	h.mu.RUnlock()

	if ok {
		if obj, ok := val.(map[string]interface{}); ok {
			return obj
		}
	}

	// fallback: load JSON file
	data, err := os.ReadFile(defaultFile)
	if err != nil {
		return map[string]interface{}{}
	}

	var result map[string]interface{}
	_ = json.Unmarshal(data, &result)

	h.mu.Lock()
	h.cache[key] = result
	h.mu.Unlock()

	return result
}
