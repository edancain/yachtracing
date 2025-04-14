package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"
)

// Waypoint represents a GPS coordinate
type Waypoint struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// YachtTelemetry represents telemetry data for a single yacht at a point in time
type YachtTelemetry struct {
	Timestamp time.Time `json:"timestamp"`
	YachtID   string    `json:"yacht_id"`
	Position  Waypoint  `json:"position"`
	Heading   float64   `json:"heading"`
	Speed     float64   `json:"speed"`
	WindSpeed float64   `json:"wind_speed"`
	WindDir   float64   `json:"wind_dir"`
}

// Generate a race course around Alcatraz
func generateAlcatrazRaceCourse() []Waypoint {
	// Alcatraz Island coordinates
	alcatraz := Waypoint{37.8270, -122.4230}

	// Create a course that circles around Alcatraz
	// We'll create points in a circle around Alcatraz with some variation
	course := make([]Waypoint, 0)

	// Parameters for the course
	radius := 0.01  // roughly 1km
	numPoints := 20 // number of points in the course

	for i := 0; i < numPoints; i++ {
		angle := 2 * math.Pi * float64(i) / float64(numPoints)

		// Add some slight randomness to make it look more natural
		radius := radius * (1 + (math.Sin(float64(i*5)) * 0.1))

		// Calculate position
		lat := alcatraz.Latitude + (math.Sin(angle) * radius)
		lng := alcatraz.Longitude + (math.Cos(angle) * radius)

		course = append(course, Waypoint{lat, lng})
	}

	// Close the loop
	course = append(course, course[0])

	return course
}

// Find the heading between two waypoints
func calculateHeading(from, to Waypoint) float64 {
	// Convert to radians
	lat1 := from.Latitude * math.Pi / 180
	lon1 := from.Longitude * math.Pi / 180
	lat2 := to.Latitude * math.Pi / 180
	lon2 := to.Longitude * math.Pi / 180

	// Calculate heading
	y := math.Sin(lon2-lon1) * math.Cos(lat2)
	x := math.Cos(lat1)*math.Sin(lat2) - math.Sin(lat1)*math.Cos(lat2)*math.Cos(lon2-lon1)
	heading := math.Atan2(y, x) * 180 / math.Pi

	// Normalize to 0-360
	if heading < 0 {
		heading += 360
	}

	return heading
}

// Calculate the distance between two waypoints in meters
func calculateDistance(from, to Waypoint) float64 {
	// Earth radius in meters
	earthRadius := 6371000.0

	// Convert to radians
	lat1 := from.Latitude * math.Pi / 180
	lon1 := from.Longitude * math.Pi / 180
	lat2 := to.Latitude * math.Pi / 180
	lon2 := to.Longitude * math.Pi / 180

	// Haversine formula
	dLat := lat2 - lat1
	dLon := lon2 - lon1
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	distance := earthRadius * c

	return distance
}

// Generate telemetry data for a yacht race
func generateRaceData() []YachtTelemetry {
	// Create race course
	course := generateAlcatrazRaceCourse()

	// Create yacht data
	yacht1Data := make([]YachtTelemetry, 0)
	yacht2Data := make([]YachtTelemetry, 0)

	// Race parameters
	// For simplicity, yacht1 starts at beginning of course
	yacht1Pos := 0
	// Yacht2 starts slightly behind
	yacht2Pos := -3

	// Simulate 30 minutes of racing with data points every 5 seconds
	startTime := time.Date(2025, 4, 14, 10, 0, 0, 0, time.UTC)
	duration := 30 * time.Minute
	interval := 5 * time.Second

	// Yacht speeds (knots)
	yacht1BaseSpeed := 7.5
	yacht2BaseSpeed := 7.8

	// Wind conditions
	baseWindSpeed := 12.0 // knots
	baseWindDir := 240.0  // degrees (wind coming from southwest)

	// Simulate the race
	for t := startTime; t.Before(startTime.Add(duration)); t = t.Add(interval) {
		// Update yacht1 position
		yacht1PosFloat := float64(yacht1Pos % len(course))
		yacht1PosInt := yacht1Pos % len(course)
		nextYacht1Pos := (yacht1PosInt + 1) % len(course)

		// Calculate heading and get current position
		pos1 := course[yacht1PosInt]
		nextPos1 := course[nextYacht1Pos]
		heading1 := calculateHeading(pos1, nextPos1)

		// Add some noise to yacht1 position (slight deviation from course)
		pos1.Latitude += (math.Sin(float64(yacht1Pos)) * 0.0002)
		pos1.Longitude += (math.Cos(float64(yacht1Pos)) * 0.0002)

		// Calculate speed with some variation
		segment := calculateDistance(pos1, nextPos1)
		speed1 := yacht1BaseSpeed * (1 + (math.Sin(float64(yacht1Pos*2)) * 0.15))

		// Add wind variation
		windSpeed := baseWindSpeed * (1 + (math.Sin(float64(yacht1Pos)) * 0.1))
		windDir := baseWindDir + (math.Cos(float64(yacht1Pos)) * 10)

		// Create telemetry data point
		yacht1Data = append(yacht1Data, YachtTelemetry{
			Timestamp: t,
			YachtID:   "America",
			Position:  pos1,
			Heading:   heading1,
			Speed:     speed1,
			WindSpeed: windSpeed,
			WindDir:   windDir,
		})

		// Move yacht1 forward based on speed
		// This is a simplified model where we advance by a fraction based on speed
		yacht1Pos += int(math.Max(1, speed1/2))

		// Update yacht2 position - similar calculation but offset
		if yacht2Pos >= 0 {
			yacht2PosInt := yacht2Pos % len(course)
			nextYacht2Pos := (yacht2PosInt + 1) % len(course)

			pos2 := course[yacht2PosInt]
			nextPos2 := course[nextYacht2Pos]
			heading2 := calculateHeading(pos2, nextPos2)

			// Add some noise to yacht2 position (slight deviation from course)
			pos2.Latitude += (math.Sin(float64(yacht2Pos*3)) * 0.0002)
			pos2.Longitude += (math.Cos(float64(yacht2Pos*3)) * 0.0002)

			// Calculate speed with some variation - yacht2 is slightly faster
			speed2 := yacht2BaseSpeed * (1 + (math.Sin(float64(yacht2Pos*2+10)) * 0.15))

			// Wind is slightly different for yacht2 due to position
			windSpeed2 := windSpeed * (1 + (math.Sin(float64(yacht2Pos)*2) * 0.05))
			windDir2 := windDir + (math.Cos(float64(yacht2Pos)*2) * 5)

			yacht2Data = append(yacht2Data, YachtTelemetry{
				Timestamp: t,
				YachtID:   "New Zealand",
				Position:  pos2,
				Heading:   heading2,
				Speed:     speed2,
				WindSpeed: windSpeed2,
				WindDir:   windDir2,
			})

			// Move yacht2 forward
			yacht2Pos += int(math.Max(1, speed2/2))
		} else {
			// Yacht2 hasn't started yet
			yacht2Pos++
		}
	}

	// Combine and sort all data by timestamp
	allData := append(yacht1Data, yacht2Data...)

	return allData
}

func main() {
	// Generate race data
	raceData := generateRaceData()

	// Save to a JSON file
	jsonData, err := json.MarshalIndent(raceData, "", "  ")
	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
		return
	}

	err = os.WriteFile("alcatraz_race_data.json", jsonData, 0644)
	if err != nil {
		fmt.Println("Error writing file:", err)
		return
	}

	fmt.Printf("Generated telemetry data for %d data points\n", len(raceData))
	fmt.Println("Race data saved to alcatraz_race_data.json")
}
