package dryer

import (
	"fmt"
	"sync"
	"time"
)

// ZoneState is the measured state of one dryer zone under the active profile.
type ZoneState struct {
	Name     string  `json:"name"`
	Target   float64 `json:"target"`
	Measured float64 `json:"measured"`
	Airflow  float64 `json:"airflow"`
	Steady   bool    `json:"steady"`
}

// DriedSpan is a meter range over which every zone was measured at steady
// state. An open span has To == 0 and ends at the current observed meter.
type DriedSpan struct {
	From float64 `json:"from"`
	To   float64 `json:"to"`
}

// Segment records the paper meters dried under one approved profile. Switching
// profile closes the segment at the current meter and opens a new one, so the
// boundary between the old and the new profile is never rewritten.
type Segment struct {
	Profile    string      `json:"profile"`
	StartMeter float64     `json:"start_meter"`
	EndMeter   float64     `json:"end_meter"`
	Closed     bool        `json:"closed"`
	Dried      []DriedSpan `json:"dried"`
	OpenedAt   time.Time   `json:"opened_at"`
	ClosedAt   time.Time   `json:"closed_at"`
}

type State struct {
	mu          sync.RWMutex
	jobID       string
	jobGen      uint64
	profile     string
	zones       []ZoneState
	segments    []Segment
	lastUpdated time.Time
	active      bool
}

func NewState() *State {
	return &State{}
}

func (state *State) Start(jobID string, generation uint64, profile Profile, startMeter float64, now time.Time) error {
	if jobID == "" || generation == 0 {
		return fmt.Errorf("dryer start parameters are incomplete")
	}
	if err := profile.Validate(); err != nil {
		return err
	}
	if startMeter < 0 {
		return fmt.Errorf("dryer start meter is invalid")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.active && state.jobID == jobID && state.jobGen == generation {
		return fmt.Errorf("dryer is already running for this job generation")
	}
	state.jobID = jobID
	state.jobGen = generation
	state.profile = profile.Name
	state.zones = zonesFromProfile(profile, nil)
	state.segments = []Segment{{Profile: profile.Name, StartMeter: startMeter, OpenedAt: now.UTC()}}
	state.lastUpdated = now.UTC()
	state.active = true
	return nil
}

func zonesFromProfile(profile Profile, previous []ZoneState) []ZoneState {
	prior := map[string]ZoneState{}
	for _, zone := range previous {
		prior[zone.Name] = zone
	}
	zones := make([]ZoneState, 0, len(profile.Zones))
	for _, zone := range profile.Zones {
		measured := 0.0
		if old, ok := prior[zone.Name]; ok {
			measured = old.Measured
		}
		zones = append(zones, ZoneState{Name: zone.Name, Target: zone.Temperature, Measured: measured, Airflow: zone.Airflow})
	}
	return zones
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

// SwitchProfile closes the segment for the running profile at meter and opens
// a fresh segment for the new approved profile. Zone setpoints come from the
// new profile; a zone shared with the old profile keeps its measured
// temperature but must reach steady state again.
func (state *State) SwitchProfile(jobID string, generation uint64, profile Profile, meter float64, now time.Time) error {
	if err := profile.Validate(); err != nil {
		return err
	}
	if meter < 0 {
		return fmt.Errorf("dryer switch meter is invalid")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.active || state.jobID != jobID || state.jobGen != generation {
		return fmt.Errorf("dryer command is stale")
	}
	if state.profile == profile.Name {
		return fmt.Errorf("dryer profile %q is already active", profile.Name)
	}
	if len(state.segments) == 0 {
		return fmt.Errorf("dryer has no active segment")
	}
	active := &state.segments[len(state.segments)-1]
	if active.Closed {
		return fmt.Errorf("dryer active segment is already closed")
	}
	state.closeSegmentLocked(active, meter, now)
	state.segments = append(state.segments, Segment{Profile: profile.Name, StartMeter: meter, OpenedAt: now.UTC()})
	state.profile = profile.Name
	state.zones = zonesFromProfile(profile, state.zones)
	state.lastUpdated = now.UTC()
	return nil
}

func (state *State) closeSegmentLocked(segment *Segment, meter float64, now time.Time) {
	if len(segment.Dried) > 0 && segment.Dried[len(segment.Dried)-1].To == 0 {
		segment.Dried[len(segment.Dried)-1].To = meter
	}
	segment.EndMeter = meter
	segment.Closed = true
	segment.ClosedAt = now.UTC()
}

// ReportMeasured records a temperature reading for a zone and latches the zone
// as steady once the reading holds within tolerance.
func (state *State) ReportMeasured(jobID string, generation uint64, zone string, temperature float64, now time.Time) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.active || state.jobID != jobID || state.jobGen != generation {
		return fmt.Errorf("dryer command is stale")
	}
	for index := range state.zones {
		if state.zones[index].Name == zone {
			state.zones[index].Measured = temperature
			state.lastUpdated = now.UTC()
			return nil
		}
	}
	return fmt.Errorf("dryer zone %q is not in the active profile", zone)
}

// MarkSteady latches a zone as steady. The ramp calls it only after the zone's
// measured temperature has held inside tolerance for the required dwell time.
func (state *State) MarkSteady(jobID string, generation uint64, zone string, now time.Time) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.active || state.jobID != jobID || state.jobGen != generation {
		return fmt.Errorf("dryer command is stale")
	}
	for index := range state.zones {
		if state.zones[index].Name == zone {
			state.zones[index].Steady = true
			state.lastUpdated = now.UTC()
			return nil
		}
	}
	return fmt.Errorf("dryer zone %q is not in the active profile", zone)
}

func (state *State) allSteadyLocked() bool {
	for _, zone := range state.zones {
		if !zone.Steady {
			return false
		}
	}
	return len(state.zones) > 0
}

// Observe advances the ledger to meter: it opens a fully-dried span once every
// zone is steady. Meters travelled before that moment stay outside every dried
// span and cannot be released as fully dried.
func (state *State) Observe(jobID string, generation uint64, meter float64, now time.Time) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.active || state.jobID != jobID || state.jobGen != generation {
		return fmt.Errorf("dryer command is stale")
	}
	if meter < 0 {
		return fmt.Errorf("dryer observed meter is invalid")
	}
	if len(state.segments) == 0 {
		return fmt.Errorf("dryer has no active segment")
	}
	segment := &state.segments[len(state.segments)-1]
	if segment.Closed {
		return fmt.Errorf("dryer active segment is closed")
	}
	steady := state.allSteadyLocked()
	openSpan := len(segment.Dried) > 0 && segment.Dried[len(segment.Dried)-1].To == 0
	switch {
	case steady && !openSpan:
		segment.Dried = append(segment.Dried, DriedSpan{From: meter})
	case !steady && openSpan:
		segment.Dried[len(segment.Dried)-1].To = meter
	}
	state.lastUpdated = now.UTC()
	return nil
}

// RangeFullyDried reports whether every meter in [from, to] is covered by a
// dried span of some segment of this job generation.
func (state *State) RangeFullyDried(jobID string, generation uint64, from, to, currentMeter float64) error {
	if to <= from {
		return fmt.Errorf("dryer dried range is invalid")
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	if state.jobID != jobID || state.jobGen != generation {
		return fmt.Errorf("dryer evidence does not belong to the active job")
	}
	type span struct{ from, to float64 }
	var spans []span
	for _, segment := range state.segments {
		for _, dried := range segment.Dried {
			end := dried.To
			if end == 0 {
				if !segment.Closed {
					end = currentMeter
				} else {
					continue
				}
			}
			if end > from {
				spans = append(spans, span{from: dried.From, to: end})
			}
		}
	}
	cover := from
	for _, current := range spans {
		if current.from > cover {
			return fmt.Errorf("dryer paper from meter %.3f was not dried at steady state", cover)
		}
		if current.to > cover {
			cover = current.to
		}
		if cover >= to {
			return nil
		}
	}
	return fmt.Errorf("dryer paper up to meter %.3f was not dried at steady state", to)
}

func (state *State) Snapshot() (string, uint64, string, []ZoneState, []Segment, bool) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	zones := append([]ZoneState(nil), state.zones...)
	segments := make([]Segment, 0, len(state.segments))
	for _, segment := range state.segments {
		segment.Dried = append([]DriedSpan(nil), segment.Dried...)
		segments = append(segments, segment)
	}
	return state.jobID, state.jobGen, state.profile, zones, segments, state.active
}
