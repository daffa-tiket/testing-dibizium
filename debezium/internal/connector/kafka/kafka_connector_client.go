package kafka

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
)

type KafkaConnectorClient struct {
	client *http.Client
}

func NewKafkaConnectorClient() *KafkaConnectorClient {
	return &KafkaConnectorClient{
		client: &http.Client{},
	}
}

func (k *KafkaConnectorClient) RegisterConnector(kafkaConnectURL, connectorName, jsonConfig string) error {
	url := fmt.Sprintf("%s/connectors", kafkaConnectURL)
	body := bytes.NewBuffer([]byte(jsonConfig))

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := k.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to register connector %s: %w", connectorName, err)
	}
	defer resp.Body.Close()

	respBody, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		fmt.Printf("Connector %s registered\n", connectorName)
	} else {
		return fmt.Errorf("failed to register connector %s: %s", connectorName, string(respBody))
	}

	return nil
}
