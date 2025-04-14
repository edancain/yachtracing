package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/edancain/yachtracing/messagebus"
)

// YachtTelemetry represents telemetry data for a single yacht at a point in time
type YachtTelemetry struct {
	Timestamp time.Time `json:"timestamp"`
	YachtID   string    `json:"yacht_id"`
	Position  struct {
		Latitude  float64 `json:"latitude"`
		Longitude float64 `json:"longitude"`
	} `json:"position"`
	Heading   float64 `json:"heading"`
	Speed     float64 `json:"speed"`
	WindSpeed float64 `json:"wind_speed"`
	WindDir   float64 `json:"wind_dir"`
}

func main() {
	// Initialize Redis-based telemetry server
	server, err := messagebus.NewTelemetryServer("localhost:6379", "./telemetry_data")
	if err != nil {
		log.Fatalf("Failed to create telemetry server: %v", err)
	}
	defer server.Close()

	// Load race data from JSON file
	raceData, err := loadRaceData("alcatraz_race_data.json")
	if err != nil {
		log.Fatalf("Failed to load race data: %v", err)
	}

	fmt.Printf("Loaded %d telemetry data points\n", len(raceData))

	// Process each yacht separately to create recording sessions
	yachtSessions := make(map[string]*messagebus.StoredSession)
	yachtIDs := make(map[string]bool)

	// First, identify all yacht IDs
	for _, data := range raceData {
		yachtIDs[data.YachtID] = true
	}

	// Create a session for each yacht
	for yachtID := range yachtIDs {
		session, err := server.StartRecordingSession(yachtID)
		if err != nil {
			log.Printf("Failed to start recording session for %s: %v", yachtID, err)
			continue
		}
		yachtSessions[yachtID] = session
		fmt.Printf("Started recording session for yacht: %s\n", yachtID)
	}

	// Process telemetry data
	for _, data := range raceData {
		// Publish updates via Redis
		publishTelemetryData(server, data)
	}

	// Stop all recording sessions
	for yachtID, session := range yachtSessions {
		if err := server.StopRecordingSession(session); err != nil {
			log.Printf("Failed to stop recording session for %s: %v", yachtID, err)
		} else {
			fmt.Printf("Stopped recording session for yacht: %s\n", yachtID)
		}
	}

	// List all available sessions
	availableSessions, err := server.ListAvailableSessions()
	if err != nil {
		log.Printf("Failed to list sessions: %v", err)
	} else {
		fmt.Println("Available telemetry sessions:")
		for _, session := range availableSessions {
			fmt.Printf("- %s\n", session)
		}
	}

	// Start a playback server on a separate port for clients to connect
	startPlaybackServer()
}

// Load race data from JSON file
func loadRaceData(filename string) ([]YachtTelemetry, error) {
	// Read the file
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Parse JSON
	var raceData []YachtTelemetry
	if err := json.Unmarshal(data, &raceData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return raceData, nil
}

// Publish telemetry data to the server
func publishTelemetryData(server *messagebus.TelemetryServer, data YachtTelemetry) {
	// Publish position as a combined value
	positionStr := fmt.Sprintf("%f,%f", data.Position.Latitude, data.Position.Longitude)
	if err := server.PublishUpdate(data.YachtID, "position", positionStr); err != nil {
		log.Printf("Failed to publish position: %v", err)
	}

	// Publish heading
	if err := server.PublishUpdate(data.YachtID, "heading", data.Heading); err != nil {
		log.Printf("Failed to publish heading: %v", err)
	}

	// Publish speed
	if err := server.PublishUpdate(data.YachtID, "speed", data.Speed); err != nil {
		log.Printf("Failed to publish speed: %v", err)
	}

	// Publish wind speed
	if err := server.PublishUpdate(data.YachtID, "wind_speed", data.WindSpeed); err != nil {
		log.Printf("Failed to publish wind speed: %v", err)
	}

	// Publish wind direction
	if err := server.PublishUpdate(data.YachtID, "wind_dir", data.WindDir); err != nil {
		log.Printf("Failed to publish wind direction: %v", err)
	}
}

// Start a playback server that simulates real-time data streaming
func startPlaybackServer() {
	fmt.Println("Telemetry data has been loaded and is available for playback")
	fmt.Println("To start playback, run the client application")
}
