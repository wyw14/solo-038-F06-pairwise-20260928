package dryer

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"
)

// ZoneState is the live, measured state of one dryer zone inside a segment.
type ZoneState struct {
	Name        string    `json:"name"`
	Target      float64   `json:"target"`
	Airflow     float64   `json:"airflow"`
	Current     float64   `json:"current"`
	StableSince time.Time `json:"stable_since,omitempty"`
	LastUpdated time.Time `json:"last_updated"`
}

// InBand reports whether the measured temperature is within tolerance of the
// zone target.
func (zone ZoneState) InBand(tolerance float64) bool {
	return math.Abs(zone.Current-zone.Target) <= tolerance
}

// Stable reports whether the zone has been measured inside the target band
// continuously since StableAt.
func (zone ZoneState) Stable(tolerance float64, now time.Time) bool {
	return zone.StableSince.After(time.Time{}) && zone.InBand(tolerance) && !now.Before(zone.StableSince)
}

// Segment is the stretch of paper dried under one approved profile. Closed
// segments are frozen; the final segment stays open until the profile changes.
type Segment struct {
	Profile    string      `json:"profile"`
	StartMeter float64     `json:"start_meter"`
	EndMeter   float64     `json:"end_meter"`
	Zones      []ZoneState `json:"zones"`
	StartedAt  time.Time   `json:"started_at"`
	ClosedAt   time.Time   `json:"closed_at,omitempty"`
}

func (segment Segment) Open() bool {
	return math.IsInf(segment.EndMeter, 1)
}

// MarshalJSON renders the open (+Inf) end meter as null so equipment
// responses stay valid JSON.
func (segment Segment) MarshalJSON() ([]byte, error) {
	type alias Segment
	payload := alias(segment)
	if segment.Open() {
		return json.Marshal(struct {
			alias
			EndMeter *float64 `json:"end_meter"`
		}{alias: payload, EndMeter: nil})
	}
	return json.Marshal(payload)
}

// Covers reports whether the meter falls inside this segment.
func (segment Segment) Covers(meter float64) bool {
	return meter >= segment.StartMeter && (segment.Open() || meter < segment.EndMeter)
}

// Ready reports whether every zone has reached steady state and held it for
// the required dwell.
func (segment Segment) Ready(tolerance float64, dwell time.Duration, now time.Time) bool {
	if len(segment.Zones) == 0 {
		return false
	}
	for _, zone := range segment.Zones {
		if !zone.InBand(tolerance) || zone.StableSince.IsZero() || now.Sub(zone.StableSince) < dwell {
			return false
		}
	}
	return true
}

type State struct {
	mu          sync.RWMutex
	jobID       string
	jobGen      uint64
	segments    []Segment
	lastUpdated time.Time
	active      bool
}

func NewState() *State {
	return &State{}
}

// Start begins drying a new job under the selected profile at startMeter. Any
// segments from a previous run are replaced.
func (state *State) Start(jobID string, generation uint64, profile Profile, startMeter float64, now time.Time) error {
	if jobID == "" || generation == 0 || startMeter < 0 {
		return fmt.Errorf("dryer start parameters are incomplete")
	}
	if err := profile.Validate(); err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.active && state.jobID == jobID && state.jobGen == generation {
		// Restart for the same job must not erase segments that already hold
		// steady-state evidence. Use SwitchProfile to change the curve.
		return nil
	}
	state.jobID = jobID
	state.jobGen = generation
	state.segments = []Segment{newSegment(profile, startMeter, now.UTC())}
	state.lastUpdated = now.UTC()
	state.active = true
	return nil
}

func newSegment(profile Profile, startMeter float64, now time.Time) Segment {
	zones := make([]ZoneState, 0, len(profile.Zones))
	for _, zone := range profile.Zones {
		zones = append(zones, ZoneState{Name: zone.Name, Target: zone.Temperature, Airflow: zone.Airflow, LastUpdated: now})
	}
	return Segment{Profile: profile.Name, StartMeter: startMeter, EndMeter: math.Inf(1), Zones: zones, StartedAt: now}
}

// SwitchProfile closes the current open segment at boundaryMeter and opens a
// new one under the next profile. Each new zone inherits the measured
// temperature of the matching old zone so heating resumes from the real
// reading instead of restarting from zero. Closed segments are never touched.
func (state *State) SwitchProfile(jobID string, generation uint64, profile Profile, boundaryMeter float64, now time.Time) error {
	if err := profile.Validate(); err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.checkJobLocked(jobID, generation); err != nil {
		return err
	}
	open, err := state.openSegmentLocked()
	if err != nil {
		return err
	}
	if boundaryMeter < open.StartMeter {
		return fmt.Errorf("dryer profile boundary moved backwards")
	}
	if open.Profile == profile.Name {
		return fmt.Errorf("dryer is already running profile %q", profile.Name)
	}
	now = now.UTC()
	open.EndMeter = boundaryMeter
	open.ClosedAt = now
	state.segments[len(state.segments)-1] = open

	next := newSegment(profile, boundaryMeter, now)
	for index := range next.Zones {
		if previous := open.Zone(next.Zones[index].Name); previous != nil {
			next.Zones[index].Current = previous.Current
			next.Zones[index].LastUpdated = previous.LastUpdated
			// Steady-state dwell carries over only when the zone setpoint is
			// unchanged; a different target must time a fresh dwell even if
			// the old reading happens to land near the new band.
			if previous.Target == next.Zones[index].Target {
				next.Zones[index].StableSince = previous.StableSince
			}
		}
	}
	state.segments = append(state.segments, next)
	state.lastUpdated = now
	return nil
}

// Zone returns a zone by name from a segment.
func (segment Segment) Zone(name string) *ZoneState {
	for index := range segment.Zones {
		if segment.Zones[index].Name == name {
			return &segment.Zones[index]
		}
	}
	return nil
}

// Observe records a measured zone temperature on the open segment. Entering
// the target band starts the steady-state dwell; leaving it clears the dwell.
func (state *State) Observe(jobID string, generation uint64, zoneName string, temperature float64, now time.Time) error {
	if temperature < 0 {
		return fmt.Errorf("dryer temperature reading is invalid")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.checkJobLocked(jobID, generation); err != nil {
		return err
	}
	open, err := state.openSegmentLocked()
	if err != nil {
		return err
	}
	zone := open.Zone(zoneName)
	if zone == nil {
		return fmt.Errorf("dryer zone %q is not part of profile %q", zoneName, open.Profile)
	}
	now = now.UTC()
	wasInBand := zone.StableSince.After(time.Time{})
	zone.Current = temperature
	zone.LastUpdated = now
	if zone.InBand(DefaultTolerance) {
		if !wasInBand {
			zone.StableSince = now
		}
	} else {
		zone.StableSince = time.Time{}
	}
	state.segments[len(state.segments)-1] = open
	state.lastUpdated = now
	return nil
}

func (state *State) Cancel(jobID string, generation uint64) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.jobID != jobID || state.jobGen != generation {
		return false
	}
	state.active = false
	return true
}

func (state *State) checkJobLocked(jobID string, generation uint64) error {
	if !state.active || state.jobID != jobID || state.jobGen != generation {
		return fmt.Errorf("dryer command is stale")
	}
	return nil
}

func (state *State) openSegmentLocked() (Segment, error) {
	if len(state.segments) == 0 {
		return Segment{}, fmt.Errorf("dryer has no active segment")
	}
	open := state.segments[len(state.segments)-1]
	if !open.Open() {
		return Segment{}, fmt.Errorf("dryer has no open segment")
	}
	return open, nil
}

// Active reports whether the dryer is running for the given job generation.
func (state *State) Active(jobID string, generation uint64) bool {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.active && state.jobID == jobID && state.jobGen == generation
}

// OpenProfile names the profile of the current open segment.
func (state *State) OpenProfile() (string, bool) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	if !state.active || len(state.segments) == 0 {
		return "", false
	}
	return state.segments[len(state.segments)-1].Profile, true
}

type SnapshotData struct {
	JobID    string
	JobGen   uint64
	Active   bool
	Profile  string
	Segments []Segment
}

func (state *State) Snapshot() SnapshotData {
	state.mu.RLock()
	defer state.mu.RUnlock()
	data := SnapshotData{JobID: state.jobID, JobGen: state.jobGen, Active: state.active, Segments: cloneSegments(state.segments)}
	if len(state.segments) > 0 {
		data.Profile = state.segments[len(state.segments)-1].Profile
	}
	return data
}

func cloneSegments(values []Segment) []Segment {
	if values == nil {
		return nil
	}
	cloned := make([]Segment, len(values))
	copy(cloned, values)
	for index := range cloned {
		cloned[index].Zones = append([]ZoneState(nil), values[index].Zones...)
	}
	return cloned
}

// SegmentAt returns the segment covering a meter position.
func (state *State) SegmentAt(meter float64) (Segment, bool) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	for _, segment := range state.segments {
		if segment.Covers(meter) {
			zones := append([]ZoneState(nil), segment.Zones...)
			segment.Zones = zones
			return segment, true
		}
	}
	return Segment{}, false
}

// SegmentsOverlapping returns every segment touching [startMeter, endMeter).
func (state *State) SegmentsOverlapping(startMeter, endMeter float64) []Segment {
	state.mu.RLock()
	defer state.mu.RUnlock()
	values := []Segment{}
	for _, segment := range state.segments {
		segmentEnd := segment.EndMeter
		if segment.Open() {
			segmentEnd = math.Inf(1)
		}
		if segment.StartMeter < endMeter && segmentEnd > startMeter {
			segment.Zones = append([]ZoneState(nil), segment.Zones...)
			values = append(values, segment)
		}
	}
	return values
}

// OpenReady reports steady state of the open segment.
func (state *State) OpenReady(tolerance float64, dwell time.Duration, now time.Time) bool {
	state.mu.RLock()
	defer state.mu.RUnlock()
	open, err := state.openSegmentLocked()
	if err != nil {
		return false
	}
	return open.Ready(tolerance, dwell, now)
}

// DefaultTolerance is the temperature band (degrees) around a zone target
// that counts as steady state.
const DefaultTolerance = 0.5

// DefaultDwell is how long every zone must keep reading inside the target
// band before a segment counts as fully dried. The simulator runs on
// compressed (millisecond) time.
const DefaultDwell = 50 * time.Millisecond

// RampZones heats every zone of the open segment toward its own target based
// on measured temperatures, and only completes once all zones have held the
// target band for dwell. It stops when cancelled, when the open profile
// changes (a switch starts a fresh ramp), or when a measurement apply fails.
func RampZones(ctx context.Context, state *State, jobID string, generation uint64, step time.Duration, tolerance float64, dwell time.Duration, apply func(zoneName string, temperature float64) error) <-chan error {
	result := make(chan error, 1)
	go func() {
		defer close(result)
		profileName, ok := state.OpenProfile()
		if !ok {
			result <- fmt.Errorf("dryer ramp has no open segment")
			return
		}
		ticker := time.NewTicker(step)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				result <- nil
				return
			case now := <-ticker.C:
				if !state.Active(jobID, generation) {
					result <- nil
					return
				}
				if current, ok := state.OpenProfile(); !ok || current != profileName {
					result <- nil
					return
				}
				snapshot := state.Snapshot()
				open := snapshot.Segments[len(snapshot.Segments)-1]
				for _, zone := range open.Zones {
					if zone.StableSince.After(time.Time{}) && zone.InBand(tolerance) {
						continue
					}
					measured := zone.Current + (zone.Target-zone.Current)*0.2
					// Already inside the band but the dwell clock was reset
					// (e.g. a profile switch kept the reading): hold target.
					if zone.InBand(tolerance) {
						measured = zone.Target
					}
					if err := apply(zone.Name, measured); err != nil {
						result <- err
						return
					}
				}
				if state.OpenReady(tolerance, dwell, now) {
					result <- nil
					return
				}
			}
		}
	}()
	return result
}
