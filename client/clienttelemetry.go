package client

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/edancain/yachtracing/messagebus"
	"github.com/go-redis/redis/v8"
)

// Helper functions to convert values to/from strings for Redis storage
func toString(value interface{}) string {
	// For simple types, just use fmt.Sprint
	switch v := value.(type) {
	case string:
		return v
	case int, int64, float64, bool:
		return fmt.Sprint(v)
	default:
		// For complex types, use JSON
		bytes, err := json.Marshal(v)
		if err != nil {
			log.Printf("Error marshaling value: %v", err)
			return fmt.Sprint(v)
		}
		return string(bytes)
	}
}

// fromString attempts to convert a string back to its original type
func fromString(str string) interface{} {
	// Try to parse as number
	if i, err := strconv.ParseInt(str, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(str, 64); err == nil {
		return f
	}
	if b, err := strconv.ParseBool(str); err == nil {
		return b
	}

	// Try to parse as JSON
	var result interface{}
	if err := json.Unmarshal([]byte(str), &result); err == nil {
		return result
	}

	// Default to string
	return str
}

// TelemetryClient handles communication with the telemetry server
// and maintains a local Redis cache
type TelemetryClient struct {
	// Redis connection
	redis      *redis.Client
	ctx        context.Context
	vehicleIDs []string
	namespace  string

	// Last known sequence number for each vehicle
	lastSeq map[string]int64

	// Callbacks for new data
	callbacks []DataCallback
}

// DataCallback is a function that receives telemetry updates
type DataCallback func(vehicleID string, key string, value interface{}, timestamp time.Time)

// NewTelemetryClient creates a new client with Redis caching capabilities
func NewTelemetryClient(redisAddr string, namespace string) (*TelemetryClient, error) {
	client := &TelemetryClient{
		redis: redis.NewClient(&redis.Options{
			Addr: redisAddr,
			DB:   0,
		}),
		ctx:        context.Background(),
		namespace:  namespace,
		vehicleIDs: make([]string, 0),
		lastSeq:    make(map[string]int64),
		callbacks:  make([]DataCallback, 0),
	}

	// Test Redis connection
	if err := client.redis.Ping(client.ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return client, nil
}

// Close releases all resources used by the client
func (c *TelemetryClient) Close() error {
	return c.redis.Close()
}

// ReconnectWithCatchup reconnects after disconnection and requests catch-up data
func (c *TelemetryClient) ReconnectWithCatchup(server *messagebus.TelemetryServer) error {
	// This would connect to your server implementation and request all messages
	// since the last known sequence number for each vehicle
	for _, vehicleID := range c.vehicleIDs {
		seq := c.lastSeq[vehicleID]

		// Request catch-up data from server
		// This is a simplified example - you would implement this in your server
		updates, err := server.GetUpdatesSince(vehicleID, seq)
		if err != nil {
			return fmt.Errorf("failed to get updates for %s: %w", vehicleID, err)
		}

		// Process each update
		for _, update := range updates {
			c.ProcessUpdate(
				update.VehicleID,
				update.Key,
				update.Value,
				update.Timestamp,
				update.Sequence,
			)
		}
	}

	return nil
}

// Subscribe adds a callback for telemetry updates
func (c *TelemetryClient) Subscribe(callback DataCallback) {
	c.callbacks = append(c.callbacks, callback)
}

// Track adds a vehicle to be tracked/cached
func (c *TelemetryClient) Track(vehicleID string) {
	for _, id := range c.vehicleIDs {
		if id == vehicleID {
			return // Already tracking
		}
	}

	c.vehicleIDs = append(c.vehicleIDs, vehicleID)

	// Get the latest sequence number from Redis or start with 0
	seq, err := c.redis.Get(c.ctx, fmt.Sprintf("%s:%s:seq", c.namespace, vehicleID)).Int64()
	if err == redis.Nil {
		c.lastSeq[vehicleID] = 0
	} else if err != nil {
		log.Printf("Error getting sequence for %s: %v", vehicleID, err)
		c.lastSeq[vehicleID] = 0
	} else {
		c.lastSeq[vehicleID] = seq
	}
}

// ProcessUpdate processes a telemetry update and stores it in Redis
func (c *TelemetryClient) ProcessUpdate(vehicleID, key string, value interface{}, timestamp time.Time, seq int64) {
	// Only process updates for vehicles we're tracking
	tracking := false
	for _, id := range c.vehicleIDs {
		if id == vehicleID {
			tracking = true
			break
		}
	}

	if !tracking {
		return
	}

	// Skip older messages (out of order delivery)
	if seq <= c.lastSeq[vehicleID] {
		return
	}

	// Update last sequence
	c.lastSeq[vehicleID] = seq

	// Store in Redis:
	// 1. Update the latest value hash
	c.redis.HSet(c.ctx, fmt.Sprintf("%s:%s:latest", c.namespace, vehicleID), key, toString(value))

	// 2. Store in time series (keep last 1000 values)
	timeKey := fmt.Sprintf("%s:%s:%s:history", c.namespace, vehicleID, key)
	member := redis.Z{
		Score:  float64(timestamp.UnixNano()),
		Member: toString(value),
	}
	c.redis.ZAdd(c.ctx, timeKey, &member)

	// 3. Keep history trimmed to last 1000 values
	c.redis.ZRemRangeByRank(c.ctx, timeKey, 0, -1001)

	// 4. Update sequence
	c.redis.Set(c.ctx, fmt.Sprintf("%s:%s:seq", c.namespace, vehicleID), seq, 0)

	// Notify callbacks
	for _, callback := range c.callbacks {
		callback(vehicleID, key, value, timestamp)
	}
}

// GetLatest gets the latest value for a key
func (c *TelemetryClient) GetLatest(vehicleID, key string) (interface{}, bool) {
	val, err := c.redis.HGet(c.ctx, fmt.Sprintf("%s:%s:latest", c.namespace, vehicleID), key).Result()
	if err == redis.Nil {
		return nil, false
	} else if err != nil {
		log.Printf("Error getting latest %s for %s: %v", key, vehicleID, err)
		return nil, false
	}

	return fromString(val), true
}

// GetHistory gets historical values for a key
func (c *TelemetryClient) GetHistory(vehicleID, key string, start, end time.Time, limit int) ([]TimeValue, error) {
	timeKey := fmt.Sprintf("%s:%s:%s:history", c.namespace, vehicleID, key)

	// Query Redis sorted set by timestamp range
	startScore := float64(start.UnixNano())
	endScore := float64(end.UnixNano())

	opts := &redis.ZRangeBy{
		Min: strconv.FormatFloat(startScore, 'f', 0, 64),
		Max: strconv.FormatFloat(endScore, 'f', 0, 64),
	}

	results, err := c.redis.ZRangeByScoreWithScores(c.ctx, timeKey, opts).Result()
	if err != nil {
		return nil, fmt.Errorf("error getting history: %w", err)
	}

	// Convert to time values
	values := make([]TimeValue, 0, len(results))
	for _, z := range results {
		timestamp := time.Unix(0, int64(z.Score))
		value := fromString(z.Member.(string))

		values = append(values, TimeValue{
			Timestamp: timestamp,
			Value:     value,
		})

		if limit > 0 && len(values) >= limit {
			break
		}
	}

	return values, nil
}

// TimeValue represents a value with a timestamp
type TimeValue struct {
	Timestamp time.Time
	Value     interface{}
}

// SearchByRange performs a range search on numerical values
func (c *TelemetryClient) SearchByRange(vehicleID, key string, min, max float64) ([]TimeValue, error) {
	// This is a more complex query that would need scanning through values
	// For demonstration, we'll pull history and filter in memory
	// In a production system, you might use Redis Streams or another approach

	// Get all values from last hour
	end := time.Now()
	start := end.Add(-1 * time.Hour)

	values, err := c.GetHistory(vehicleID, key, start, end, 0)
	if err != nil {
		return nil, fmt.Errorf("error getting history for range search: %w", err)
	}

	// Filter by range
	result := make([]TimeValue, 0)
	for _, tv := range values {
		// Try to convert to float64 for comparison
		var val float64
		switch v := tv.Value.(type) {
		case float64:
			val = v
		case int:
			val = float64(v)
		case string:
			parsed, err := strconv.ParseFloat(v, 64)
			if err != nil {
				continue // Skip non-numeric values
			}
			val = parsed
		default:
			continue // Skip non-numeric values
		}

		// Check range
		if val >= min && val <= max {
			result = append(result, tv)
		}
	}

	return result, nil
}
