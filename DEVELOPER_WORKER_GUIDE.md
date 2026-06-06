# MQueue Microservice Developer & Worker Guide

This guide describes integration patterns and provides code examples for writing consumer workers that interface with MQueue.

---

## 1. At-Least-Once Delivery & Idempotency Invariants

MQueue guarantees **at-least-once delivery**. This means that under network partitions, worker crashes, or database lock timeouts, a message may occasionally be delivered to more than one consumer worker.

### 1.1. Designing Idempotent Workers
Consumers **must** be idempotent. Before executing a task, check if the task's Snowflake ID has already been successfully processed by your microservice.

#### Recommended Database Pattern:
```sql
-- Create a processed_tasks audit log table in your service's database
CREATE TABLE processed_tasks (
    task_id BIGINT PRIMARY KEY,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- When processing, run a transaction:
BEGIN;
INSERT INTO processed_tasks (task_id) VALUES ($1);
-- If the insert fails with duplicate key, skip processing (it is a duplicate delivery!)
-- Else, perform the business logic (e.g. process payment)
COMMIT;
```

---

## 2. Worker Implementation Pattern (Go Example)

A robust worker should query `/dequeue`, process items, and call `/ack` on success or `/nack` on failure.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	mqueueURL = "http://localhost:8080"
	jwtToken  = "YOUR_SIGNED_JWT_TOKEN"
	namespace = "payments"
	topic     = "process-refund"
)

type MQueueItem struct {
	ID             int64  `json:"id"`
	Payload        string `json:"payload"` // Base64 encoded payload
	LeaseExpiresAt string `json:"lease_expires_at"`
}

func main() {
	client := &http.Client{Timeout: 10 * time.Second}

	for {
		items, err := dequeueBatch(client, 5)
		if err != nil {
			fmt.Printf("Error dequeueing: %v\n", err)
			time.Sleep(2 * time.Second) // Back off on connection failures
			continue
		}

		if len(items) == 0 {
			time.Sleep(1 * time.Second) // Wait for new items
			continue
		}

		for _, item := range items {
			processItem(client, item)
		}
	}
}

func dequeueBatch(client *http.Client, limit int) ([]MQueueItem, error) {
	req, _ := http.NewRequest("GET", fmt.Sprintf("%s/dequeue?namespace=%s&topic=%s&limit=%d", mqueueURL, namespace, topic, limit), nil)
	req.Header.Set("Authorization", "Bearer "+jwtToken)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("bad status %d: %s", resp.StatusCode, string(body))
	}

	var items []MQueueItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		return nil, err
	}
	return items, nil
}

func processItem(client *http.Client, item MQueueItem) {
	fmt.Printf("Processing item %d...\n", item.ID)

	// Simulate work
	err := doWork(item.Payload)
	if err != nil {
		fmt.Printf("Failed processing item %d: %v. Sending NACK...\n", item.ID, err)
		_ = respond(client, "/nack", map[string]interface{}{
			"id":        item.ID,
			"namespace": namespace,
			"topic":     topic,
			"error":     err.Error(),
		})
		return
	}

	fmt.Printf("Successfully processed item %d. Sending ACK...\n", item.ID)
	_ = respond(client, "/ack", map[string]interface{}{
		"id":        item.ID,
		"namespace": namespace,
		"topic":     topic,
	})
}

func doWork(payloadBase64 string) error {
	// Base64 decode and process your business logic here
	return nil
}

func respond(client *http.Client, path string, body interface{}) error {
	data, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", mqueueURL+path, bytes.NewBuffer(data))
	req.Header.Set("Authorization", "Bearer "+jwtToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
```

---

## 3. Visibility Timeout Configuration

The Visibility Timeout (`lease_expires_at`) prevents other workers from consuming a message while a worker is currently processing it.

* **Choose the Right Timeout**: Set visibility timeout to at least 2 times the maximum expected execution time of your task.
* **If visibility timeout is too short**: MQueue will expire the lease and deliver the message to another worker *while* the first worker is still processing, leading to duplicate execution and database locks.
* **If visibility timeout is too long**: If your worker crashes, the message will remain locked and hidden from other workers for a long time, increasing queue delay.
