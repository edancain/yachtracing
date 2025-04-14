package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/edancain/yachtracing/messagebus"
)

// Simulated yacht data
type YachtData struct {
	ID        string
	Speed     float64
	Heading   float64
	Longitude float64
	Latitude  float64
	WindSpeed float64
}

func main() {
	// Initialize the telemetry store with a storage directory
	store, err := messagebus.NewTelemetryStore("./yacht_data")
	if err != nil {
		log.Fatalf("Failed to create telemetry store: %v", err)
	}

	// Demo options
	demoMode := "record" // Change to "playback" to demonstrate playback

	switch demoMode {
	case "record":
		simulateRace(store)
	case "playback":
		playbackRace(store)
	}
}

// simulateRace simulates a yacht race with multiple yachts
func simulateRace(store *messagebus.TelemetryStore) {
	// Create 6 yachts for our simulated race
	yachtCount := 6
	yachts := make([]*YachtData, yachtCount)
	sessions := make([]*messagebus.StoredSession, yachtCount)

	fmt.Println("Starting race simulation with", yachtCount, "yachts")

	// Initialize yacht data and start recording sessions
	for i := 0; i < yachtCount; i++ {
		yachts[i] = &YachtData{
			ID:        fmt.Sprintf("yacht-%d", i+1),
			Speed:     5.0 + rand.Float64()*3.0, // 5-8 knots starting speed
			Heading:   rand.Float64() * 360,     // Random initial heading
			Longitude: -122.4 + rand.Float64()*0.1,
			Latitude:  37.8 + rand.Float64()*0.1,
			WindSpeed: 10.0 + rand.Float64()*5.0,
		}

		// Start recording session for this yacht
		sessions[i] = store.StartRecordingSession(yachts[i].ID)
		fmt.Printf("Started recording session for %s: %s\n", yachts[i].ID, sessions[i].SessionID)
	}

	// Simulate a 2-minute race (in a real race, this would be 60 minutes)
	raceTime := 2 * time.Minute
	updateInterval := 500 * time.Millisecond
	iterations := int(raceTime / updateInterval)

	fmt.Printf("Simulating race for %v with updates every %v\n", raceTime, updateInterval)

	// Main simulation loop
	for iter := 0; iter < iterations; iter++ {
		// Update each yacht
		for i, yacht := range yachts {
			// Simulate random changes
			yacht.Speed += (rand.Float64()*0.6 - 0.3) // Speed change +/- 0.3 knots
			yacht.Heading += (rand.Float64()*10 - 5)  // Heading change +/- 5 degrees
			yacht.WindSpeed += (rand.Float64() - 0.5) // Wind change +/- 0.5 knots

			// Wrap heading to 0-360
			if yacht.Heading < 0 {
				yacht.Heading += 360
			} else if yacht.Heading >= 360 {
				yacht.Heading -= 360
			}

			// Update position based on speed and heading
			headingRad := yacht.Heading * (3.14159 / 180.0)
			yacht.Longitude += 0.00001 * yacht.Speed * math.Cos(headingRad) * float64(time.Since(sessions[i].StartTime)/time.Hour)
			yacht.Latitude += 0.00001 * yacht.Speed * math.Sin(headingRad) * float64(time.Since(sessions[i].StartTime)/time.Hour)

			// Send data updates
			store.UpdateLiveData(yacht.ID, "speed", yacht.Speed)
			store.UpdateLiveData(yacht.ID, "heading", yacht.Heading)
			store.UpdateLiveData(yacht.ID, "position", fmt.Sprintf("%.6f,%.6f", yacht.Latitude, yacht.Longitude))
			store.UpdateLiveData(yacht.ID, "wind_speed", yacht.WindSpeed)
		}

		// Status update every 5 seconds
		if iter%(5*1000/int(updateInterval.Milliseconds())) == 0 {
			elapsed := updateInterval * time.Duration(iter)
			fmt.Printf("Race time: %v / %v (%.1f%%)\n", elapsed, raceTime, float64(elapsed)/float64(raceTime)*100)
		}

		// Wait for next update
		time.Sleep(updateInterval)
	}

	// End the race and stop all recording sessions
	fmt.Println("Race finished! Saving data...")
	for i, session := range sessions {
		if err := store.StopRecordingSession(session); err != nil {
			fmt.Printf("Error saving session for %s: %v\n", yachts[i].ID, err)
		} else {
			fmt.Printf("Successfully saved session for %s\n", yachts[i].ID)
		}
	}

	// List all available sessions
	availableSessions, err := store.ListSessions()
	if err != nil {
		fmt.Printf("Error listing sessions: %v\n", err)
	} else {
		fmt.Println("Available sessions:")
		for _, s := range availableSessions {
			fmt.Printf("- %s\n", s)
		}
	}
}

// playbackRace demonstrates playing back a previously recorded race
func playbackRace(store *messagebus.TelemetryStore) {
	// List available sessions
	availableSessions, err := store.ListSessions()
	if err != nil {
		log.Fatalf("Error listing sessions: %v", err)
	}

	if len(availableSessions) == 0 {
		log.Fatalf("No sessions available for playback. Run in 'record' mode first.")
	}

	fmt.Println("Available sessions:")
	for i, s := range availableSessions {
		fmt.Printf("%d. %s\n", i+1, s)
	}

	// For this example, we'll just use the first session
	// In a real app, you might let the user choose
	sessionID := availableSessions[0]

	fmt.Printf("Loading session: %s\n", sessionID)
	session, err := store.LoadSessionFromDisk(sessionID)
	if err != nil {
		log.Fatalf("Error loading session: %v", err)
	}

	// Create a playback controller
	controller := messagebus.NewPlaybackController(session)

	// Subscribe to playback events
	controller.Subscribe(func(vehicleID string, timestamp time.Time, key string, value interface{}) {
		fmt.Printf("[%s] %s - %s: %v\n", timestamp.Format("15:04:05.000"), vehicleID, key, value)
	})

	// Start playback at 2x speed
	controller.SetSpeed(2.0)
	fmt.Println("Starting playback at 2x speed")
	controller.Start()

	// Wait for playback to finish
	for controller.IsPlaying {
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("Playback complete!")
}
