package messagebus

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// TelemetryServer handles both real-time data streaming and historical data access
type TelemetryServer struct {
	store      *TelemetryStore // For persistent storage
	redis      *redis.Client   // For real-time distribution and caching
	ctx        context.Context
	sequencers map[string]int64 // Keeps track of sequence numbers by vehicle
	mu         sync.RWMutex
}

// TelemetryUpdate represents a single update with sequence information
type TelemetryUpdate struct {
	VehicleID string
	Key       string
	Value     interface{}
	Timestamp time.Time
	Sequence  int64
}

// NewTelemetryServer creates a new server with Redis and persistent storage
func NewTelemetryServer(redisAddr, storageDir string) (*TelemetryServer, error) {
	// Create storage directory
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	// Create telemetry store
	store, err := NewTelemetryStore(storageDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create telemetry store: %w", err)
	}

	// Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   0,
	})

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &TelemetryServer{
		store:      store,
		redis:      rdb,
		ctx:        ctx,
		sequencers: make(map[string]int64),
	}, nil
}

// Close releases all resources
func (ts *TelemetryServer) Close() error {
	if err := ts.redis.Close(); err != nil {
		return err
	}
	return nil
}

// PublishUpdate publishes a telemetry update
func (ts *TelemetryServer) PublishUpdate(vehicleID, key string, value interface{}) error {
	now := time.Now()

	// Get and increment sequence number
	ts.mu.Lock()
	seq := ts.sequencers[vehicleID] + 1
	ts.sequencers[vehicleID] = seq
	ts.mu.Unlock()

	// Create update
	update := TelemetryUpdate{
		VehicleID: vehicleID,
		Key:       key,
		Value:     value,
		Timestamp: now,
		Sequence:  seq,
	}

	// Publish to Redis
	channel := fmt.Sprintf("telemetry:%s", vehicleID)
	bytes, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("failed to marshal update: %w", err)
	}

	if err := ts.redis.Publish(ts.ctx, channel, bytes).Err(); err != nil {
		return fmt.Errorf("failed to publish update: %w", err)
	}

	// Store in Redis for catch-up
	key = fmt.Sprintf("telemetry:%s:updates", vehicleID)
	member := redis.Z{
		Score:  float64(seq),
		Member: string(bytes),
	}
	if err := ts.redis.ZAdd(ts.ctx, key, &member).Err(); err != nil {
		return fmt.Errorf("failed to store update: %w", err)
	}

	// Keep only the last 1000 updates per vehicle
	if err := ts.redis.ZRemRangeByRank(ts.ctx, key, 0, -1001).Err(); err != nil {
		return fmt.Errorf("failed to trim updates: %w", err)
	}

	// Update live data in store
	ts.store.UpdateLiveData(vehicleID, key, value)

	return nil
}

// GetUpdatesSince retrieves updates since a given sequence number
func (ts *TelemetryServer) GetUpdatesSince(vehicleID string, seq int64) ([]TelemetryUpdate, error) {
	key := fmt.Sprintf("telemetry:%s:updates", vehicleID)

	// Query Redis sorted set
	opts := &redis.ZRangeBy{
		Min: fmt.Sprintf("(%d", seq), // Exclusive of seq
		Max: "+inf",                  // No upper bound
	}

	results, err := ts.redis.ZRangeByScore(ts.ctx, key, opts).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get updates: %w", err)
	}

	// Parse results
	updates := make([]TelemetryUpdate, 0, len(results))
	for _, str := range results {
		var update TelemetryUpdate
		if err := json.Unmarshal([]byte(str), &update); err != nil {
			return nil, fmt.Errorf("failed to unmarshal update: %w", err)
		}
		updates = append(updates, update)
	}

	return updates, nil
}

// StartRecordingSession begins recording telemetry for a vehicle
func (ts *TelemetryServer) StartRecordingSession(vehicleID string) (*StoredSession, error) {
	return ts.store.StartRecordingSession(vehicleID), nil
}

// StopRecordingSession stops a recording session
func (ts *TelemetryServer) StopRecordingSession(session *StoredSession) error {
	return ts.store.StopRecordingSession(session)
}

// ListAvailableSessions lists all available recorded sessions
func (ts *TelemetryServer) ListAvailableSessions() ([]string, error) {
	return ts.store.ListSessions()
}

// LoadSession loads a session from disk
func (ts *TelemetryServer) LoadSession(sessionID string) (*StoredSession, error) {
	return ts.store.LoadSessionFromDisk(sessionID)
}

// CreatePlayback creates a playback controller for a session
func (ts *TelemetryServer) CreatePlayback(session *StoredSession) *PlaybackController {
	return NewPlaybackController(session)
}

// RetrieveLiveData gets the current value for a specific key
func (ts *TelemetryServer) RetrieveLiveData(vehicleID, key string) (interface{}, bool) {
	return ts.store.GetLiveData(vehicleID, key)
}

// SubscribeToLiveUpdates subscribes to updates for a vehicle
// Returns a channel that must be closed by the caller
func (ts *TelemetryServer) SubscribeToLiveUpdates(vehicleID string) (<-chan TelemetryUpdate, func(), error) {
	// Subscribe to Redis channel
	channel := fmt.Sprintf("telemetry:%s", vehicleID)
	pubsub := ts.redis.Subscribe(ts.ctx, channel)

	// Test subscription
	if _, err := pubsub.Receive(ts.ctx); err != nil {
		pubsub.Close()
		return nil, nil, fmt.Errorf("failed to subscribe: %w", err)
	}

	// Create channel for updates
	updates := make(chan TelemetryUpdate, 100)

	// Start goroutine to handle messages
	go func() {
		defer close(updates)
		ch := pubsub.Channel()

		for msg := range ch {
			var update TelemetryUpdate
			if err := json.Unmarshal([]byte(msg.Payload), &update); err != nil {
				continue
			}

			// Send update to channel (non-blocking)
			select {
			case updates <- update:
				// Update sent
			default:
				// Channel full, discard update
			}
		}
	}()

	// Return cleanup function
	cleanup := func() {
		pubsub.Close()
	}

	return updates, cleanup, nil
}

// BackupDataToFile exports the current live data to a file
func (ts *TelemetryServer) BackupDataToFile(filename string) error {
	// Lock the data to get a consistent snapshot
	ts.store.LiveData.mu.RLock()
	defer ts.store.LiveData.mu.RUnlock()

	// Create file
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer file.Close()

	// Write data as JSON
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(ts.store.LiveData.Data); err != nil {
		return fmt.Errorf("failed to encode data: %w", err)
	}

	return nil
}

// RestoreDataFromFile loads data from a backup file
func (ts *TelemetryServer) RestoreDataFromFile(filename string) error {
	// Open file
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %w", err)
	}
	defer file.Close()

	// Read data
	var data map[string]interface{}
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&data); err != nil {
		return fmt.Errorf("failed to decode data: %w", err)
	}

	// Lock data dictionary and restore
	ts.store.LiveData.mu.Lock()
	defer ts.store.LiveData.mu.Unlock()

	ts.store.LiveData.Data = data

	return nil
}

// RegularBackupWorker runs periodic backups of the live data
func (ts *TelemetryServer) RegularBackupWorker(backupDir string, interval time.Duration) {
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		fmt.Printf("Failed to create backup directory: %v\n", err)
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for tick := range ticker.C {
		// Create backup filename with timestamp
		filename := filepath.Join(backupDir, fmt.Sprintf("backup_%s.json", tick.Format("20060102_150405")))

		if err := ts.BackupDataToFile(filename); err != nil {
			fmt.Printf("Backup failed: %v\n", err)
		} else {
			fmt.Printf("Backup created: %s\n", filename)
		}

		// Keep only the last 10 backups
		files, err := filepath.Glob(filepath.Join(backupDir, "backup_*.json"))
		if err != nil {
			continue
		}

		if len(files) > 10 {
			// Sort files by name (which includes timestamp)
			for i := 0; i < len(files)-10; i++ {
				os.Remove(files[i])
			}
		}
	}
}
