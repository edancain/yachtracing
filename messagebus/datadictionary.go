package messagebus

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DataDictionary maintains the current state of all telemetry data
type DataDictionary struct {
	Data map[string]interface{}
	mu   sync.RWMutex
}

// TelemetryStore handles both live data and historical sessions
type TelemetryStore struct {
	LiveData       *DataDictionary
	StoredSessions map[string][]*StoredSession // VehicleID -> sessions
	storageDir     string
	mu             sync.RWMutex
}

// StoredSession represents a single recording session for a vehicle
type StoredSession struct {
	SessionID   string
	VehicleID   string
	StartTime   time.Time
	EndTime     time.Time
	DataPoints  []DataPoint
	currentIdx  int // For playback
	isRecording bool
}

// DataPoint represents a single point of telemetry data with a timestamp
type DataPoint struct {
	Timestamp time.Time
	Data      map[string]interface{}
}

// NewTelemetryStore creates a new telemetry store with persistence capabilities
func NewTelemetryStore(storageDir string) (*TelemetryStore, error) {
	// Create storage directory if it doesn't exist
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	return &TelemetryStore{
		LiveData:       &DataDictionary{Data: make(map[string]interface{})},
		StoredSessions: make(map[string][]*StoredSession),
		storageDir:     storageDir,
	}, nil
}

// UpdateLiveData updates the live data dictionary and records to active sessions
func (ts *TelemetryStore) UpdateLiveData(vehicleID, key string, value interface{}) {
	fullKey := fmt.Sprintf("%s.%s", vehicleID, key)

	// Update live data
	ts.LiveData.mu.Lock()
	ts.LiveData.Data[fullKey] = value
	ts.LiveData.mu.Unlock()

	// Record to any active sessions for this vehicle
	ts.mu.RLock()
	sessions, exists := ts.StoredSessions[vehicleID]
	ts.mu.RUnlock()

	if exists {
		now := time.Now()
		for _, session := range sessions {
			if session.isRecording {
				session.addDataPoint(key, value, now)
			}
		}
	}
}

// GetLiveData retrieves a value from the live data dictionary
func (ts *TelemetryStore) GetLiveData(vehicleID, key string) (interface{}, bool) {
	fullKey := fmt.Sprintf("%s.%s", vehicleID, key)

	ts.LiveData.mu.RLock()
	defer ts.LiveData.mu.RUnlock()

	value, exists := ts.LiveData.Data[fullKey]
	return value, exists
}

// StartRecordingSession begins a new recording session for a vehicle
func (ts *TelemetryStore) StartRecordingSession(vehicleID string) *StoredSession {
	session := &StoredSession{
		SessionID:   fmt.Sprintf("%s-%d", vehicleID, time.Now().Unix()),
		VehicleID:   vehicleID,
		StartTime:   time.Now(),
		DataPoints:  make([]DataPoint, 0),
		isRecording: true,
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	if _, exists := ts.StoredSessions[vehicleID]; !exists {
		ts.StoredSessions[vehicleID] = make([]*StoredSession, 0)
	}

	ts.StoredSessions[vehicleID] = append(ts.StoredSessions[vehicleID], session)
	return session
}

// StopRecordingSession ends a recording session and saves it to disk
func (ts *TelemetryStore) StopRecordingSession(session *StoredSession) error {
	session.EndTime = time.Now()
	session.isRecording = false

	// Save to disk
	return ts.saveSessionToDisk(session)
}

// saveSessionToDisk persists a session to disk in JSON format
func (ts *TelemetryStore) saveSessionToDisk(session *StoredSession) error {
	filename := filepath.Join(ts.storageDir, fmt.Sprintf("%s.json", session.SessionID))

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create session file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(session); err != nil {
		return fmt.Errorf("failed to encode session data: %w", err)
	}

	return nil
}

// LoadSessionFromDisk loads a previously recorded session from disk
func (ts *TelemetryStore) LoadSessionFromDisk(sessionID string) (*StoredSession, error) {
	filename := filepath.Join(ts.storageDir, fmt.Sprintf("%s.json", sessionID))

	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open session file: %w", err)
	}
	defer file.Close()

	var session StoredSession
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&session); err != nil {
		return nil, fmt.Errorf("failed to decode session data: %w", err)
	}

	// Add to our in-memory sessions
	ts.mu.Lock()
	if _, exists := ts.StoredSessions[session.VehicleID]; !exists {
		ts.StoredSessions[session.VehicleID] = make([]*StoredSession, 0)
	}
	ts.StoredSessions[session.VehicleID] = append(ts.StoredSessions[session.VehicleID], &session)
	ts.mu.Unlock()

	return &session, nil
}

// ListSessions returns all available session files
func (ts *TelemetryStore) ListSessions() ([]string, error) {
	files, err := os.ReadDir(ts.storageDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read storage directory: %w", err)
	}

	sessions := make([]string, 0)
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".json" {
			sessions = append(sessions, file.Name()[:len(file.Name())-5]) // Remove .json
		}
	}

	return sessions, nil
}

// addDataPoint adds a data point to a session
func (s *StoredSession) addDataPoint(key string, value interface{}, timestamp time.Time) {
	// See if we need to create a new data point or update the latest one
	if len(s.DataPoints) == 0 || time.Since(s.DataPoints[len(s.DataPoints)-1].Timestamp) > 100*time.Millisecond {
		// Create new data point if none exist or latest one is older than 100ms
		s.DataPoints = append(s.DataPoints, DataPoint{
			Timestamp: timestamp,
			Data:      map[string]interface{}{key: value},
		})
	} else {
		// Update the latest data point
		s.DataPoints[len(s.DataPoints)-1].Data[key] = value
	}
}

// PlaybackController manages replaying a recorded session
type PlaybackController struct {
	Session        *StoredSession
	Speed          float64 // Playback speed multiplier
	CurrentIndex   int
	IsPlaying      bool
	PlaybackTime   time.Time // Virtual playback time
	RealStartTime  time.Time // When playback was started
	Subscribers    []PlaybackSubscriber
	playbackTicker *time.Ticker
	mu             sync.RWMutex
}

// PlaybackSubscriber receives playback data
type PlaybackSubscriber func(vehicleID string, timestamp time.Time, key string, value interface{})

// NewPlaybackController creates a new playback controller for a session
func NewPlaybackController(session *StoredSession) *PlaybackController {
	return &PlaybackController{
		Session:      session,
		Speed:        1.0, // Normal speed
		CurrentIndex: 0,
		IsPlaying:    false,
		Subscribers:  make([]PlaybackSubscriber, 0),
	}
}

// Subscribe adds a subscriber to the playback
func (pc *PlaybackController) Subscribe(sub PlaybackSubscriber) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.Subscribers = append(pc.Subscribers, sub)
}

// SetSpeed changes the playback speed
func (pc *PlaybackController) SetSpeed(speed float64) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if speed <= 0 {
		speed = 0.1 // Minimum speed
	}

	pc.Speed = speed

	// If playing, restart the ticker with new speed
	if pc.IsPlaying {
		pc.stop()
		pc.start()
	}
}

// Start begins playback
func (pc *PlaybackController) Start() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.IsPlaying {
		return
	}

	pc.IsPlaying = true
	pc.RealStartTime = time.Now()

	if pc.CurrentIndex < len(pc.Session.DataPoints) {
		pc.PlaybackTime = pc.Session.DataPoints[pc.CurrentIndex].Timestamp
	} else {
		pc.CurrentIndex = 0
		pc.PlaybackTime = pc.Session.StartTime
	}

	pc.start()
}

// stop is a helper to stop the ticker
func (pc *PlaybackController) stop() {
	if pc.playbackTicker != nil {
		pc.playbackTicker.Stop()
		pc.playbackTicker = nil
	}
}

// start is a helper to start the ticker
func (pc *PlaybackController) start() {
	// Check ticker interval at 10 times per second
	pc.playbackTicker = time.NewTicker(100 * time.Millisecond)

	go func() {
		for range pc.playbackTicker.C {
			if !pc.tick() {
				pc.stop()
				break
			}
		}
	}()
}

// Pause pauses playback
func (pc *PlaybackController) Pause() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if !pc.IsPlaying {
		return
	}

	pc.IsPlaying = false
	pc.stop()
}

// Seek moves the playback position to a specific time
func (pc *PlaybackController) Seek(targetTime time.Time) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// Find the closest data point to the target time
	for i, dp := range pc.Session.DataPoints {
		if dp.Timestamp.After(targetTime) || dp.Timestamp.Equal(targetTime) {
			pc.CurrentIndex = i
			pc.PlaybackTime = dp.Timestamp
			return
		}
	}

	// If we get here, target time is after last data point
	pc.CurrentIndex = len(pc.Session.DataPoints) - 1
	if pc.CurrentIndex >= 0 {
		pc.PlaybackTime = pc.Session.DataPoints[pc.CurrentIndex].Timestamp
	}
}

// tick advances playback one step
func (pc *PlaybackController) tick() bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if !pc.IsPlaying || pc.CurrentIndex >= len(pc.Session.DataPoints) {
		pc.IsPlaying = false
		return false
	}

	// Calculate how much virtual time has elapsed
	realElapsed := time.Since(pc.RealStartTime)
	virtualElapsed := time.Duration(float64(realElapsed) * pc.Speed)
	targetTime := pc.PlaybackTime.Add(virtualElapsed)

	// Send any data points that should have been sent by now
	var sentAny bool
	for pc.CurrentIndex < len(pc.Session.DataPoints) {
		dp := pc.Session.DataPoints[pc.CurrentIndex]

		if dp.Timestamp.After(targetTime) {
			break
		}

		// Send this data point
		for key, value := range dp.Data {
			for _, sub := range pc.Subscribers {
				sub(pc.Session.VehicleID, dp.Timestamp, key, value)
			}
		}

		pc.CurrentIndex++
		sentAny = true
	}

	// If we reached the end
	if pc.CurrentIndex >= len(pc.Session.DataPoints) {
		pc.IsPlaying = false
		return false
	}

	// If we sent data, update the playback time
	if sentAny {
		pc.PlaybackTime = pc.Session.DataPoints[pc.CurrentIndex-1].Timestamp
	}

	return true
}
