package dryer

import (
	"context"
	"testing"
	"time"
)

func twoZoneProfile(name string, entry, exit float64) Profile {
	return Profile{Name: name, Zones: []Zone{{Name: "entry", Temperature: entry, Airflow: 20}, {Name: "exit", Temperature: exit, Airflow: 24}}}
}

// markStable drives every zone of the open segment straight to its measured
// target so the steady-state dwell can elapse.
func markStable(t *testing.T, state *State, jobID string, generation uint64, segments []Segment, now time.Time) {
	t.Helper()
	open := segments[len(segments)-1]
	for _, zone := range open.Zones {
		if err := state.Observe(jobID, generation, zone.Name, zone.Target, now); err != nil {
			t.Fatalf("observe %s: %v", zone.Name, err)
		}
	}
}

func TestRampZonesHeatsEveryZoneAndWaitsForSteadyState(t *testing.T) {
	state := NewState()
	profile := twoZoneProfile("production", 80, 100)
	base := time.Now()
	if err := state.Start("job-1", 1, profile, 0, base); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := RampZones(ctx, state, "job-1", 1, time.Millisecond, 0.5, 20*time.Millisecond, func(zone string, value float64) error {
		return state.Observe("job-1", 1, zone, value, time.Now())
	})
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ramp: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ramp never reached steady state")
	}
	if !state.OpenReady(0.5, 20*time.Millisecond, time.Now()) {
		t.Fatal("all zones must be steady after ramp completes")
	}
	snapshot := state.Snapshot()
	open := snapshot.Segments[0]
	for _, zone := range open.Zones {
		if zone.Current < zone.Target-0.5 || zone.Current > zone.Target+0.5 {
			t.Fatalf("zone %s measured %v, want target %v", zone.Name, zone.Current, zone.Target)
		}
	}
}

func TestObserveLeavingBandResetsSteadyState(t *testing.T) {
	state := NewState()
	now := time.Now()
	if err := state.Start("job-1", 1, twoZoneProfile("p", 80, 100), 0, now); err != nil {
		t.Fatal(err)
	}
	if err := state.Observe("job-1", 1, "entry", 80, now); err != nil {
		t.Fatal(err)
	}
	if err := state.Observe("job-1", 1, "entry", 80, now.Add(10*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	segment, _ := state.SegmentAt(0)
	if segment.Zone("entry").StableSince.IsZero() {
		t.Fatal("entry should have started its dwell")
	}
	if err := state.Observe("job-1", 1, "entry", 60, now.Add(20*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	segment, _ = state.SegmentAt(0)
	if !segment.Zone("entry").StableSince.IsZero() {
		t.Fatal("leaving the target band must reset steady state")
	}
}

func TestStaleGenerationObserveRejected(t *testing.T) {
	state := NewState()
	if err := state.Start("job-1", 2, twoZoneProfile("p", 80, 100), 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := state.Observe("job-1", 1, "entry", 80, time.Now()); err == nil {
		t.Fatal("stale generation observation must be rejected")
	}
}

func TestSwitchProfilePreservesBoundaryAndOldSegment(t *testing.T) {
	state := NewState()
	first := twoZoneProfile("production", 80, 100)
	base := time.Now()
	if err := state.Start("job-1", 1, first, 0, base); err != nil {
		t.Fatal(err)
	}
	markStable(t, state, "job-1", 1, state.Snapshot().Segments, base.Add(time.Millisecond))

	second := twoZoneProfile("heavy-stock", 80, 110)
	boundary := 120.0
	if err := state.SwitchProfile("job-1", 1, second, boundary, base.Add(50*time.Millisecond)); err != nil {
		t.Fatalf("switch: %v", err)
	}

	snapshot := state.Snapshot()
	if len(snapshot.Segments) != 2 {
		t.Fatalf("want 2 preserved segments, got %d", len(snapshot.Segments))
	}
	oldSegment := snapshot.Segments[0]
	newSegment := snapshot.Segments[1]
	if oldSegment.Open() {
		t.Fatal("old segment must be closed")
	}
	if oldSegment.StartMeter != 0 || oldSegment.EndMeter != boundary {
		t.Fatalf("old segment boundary = %v..%v, want 0..%v", oldSegment.StartMeter, oldSegment.EndMeter, boundary)
	}
	if oldSegment.Profile != "production" || !oldSegment.Ready(0.5, 0, base.Add(50*time.Millisecond)) {
		t.Fatal("old segment must keep its steady-state evidence")
	}
	if !newSegment.Open() || newSegment.StartMeter != boundary || newSegment.Profile != "heavy-stock" {
		t.Fatalf("new segment not opened on the boundary: %+v", newSegment)
	}
	// entry target unchanged (80): reading and dwell carry over.
	entry := newSegment.Zone("entry")
	if entry.Current != 80 || entry.StableSince.IsZero() {
		t.Fatalf("unchanged entry zone must inherit measured steady temperature, got %+v", entry)
	}
	// exit target changed 100 -> 110: reading is inherited, dwell reset.
	exit := newSegment.Zone("exit")
	if exit.Current != 100 || !exit.StableSince.IsZero() {
		t.Fatalf("changed exit zone must inherit reading but reset dwell, got %+v", exit)
	}
	// Meter lookup still resolves the preserved old segment.
	if segment, ok := state.SegmentAt(50); !ok || segment.Profile != "production" {
		t.Fatal("meters before the boundary must still map to the old profile")
	}
	if segment, ok := state.SegmentAt(boundary); !ok || segment.Profile != "heavy-stock" {
		t.Fatal("meters at/after the boundary must map to the new profile")
	}
}

func TestSwitchProfileRejectsBackwardsBoundary(t *testing.T) {
	state := NewState()
	if err := state.Start("job-1", 1, twoZoneProfile("a", 80, 100), 10, time.Now()); err != nil {
		t.Fatal(err)
	}
	err := state.SwitchProfile("job-1", 1, twoZoneProfile("b", 70, 90), 5, time.Now())
	if err == nil {
		t.Fatal("boundary before the segment start must be rejected")
	}
}

func TestRestartSameJobDoesNotEraseSegments(t *testing.T) {
	state := NewState()
	profile := twoZoneProfile("production", 80, 100)
	now := time.Now()
	if err := state.Start("job-1", 1, profile, 0, now); err != nil {
		t.Fatal(err)
	}
	if err := state.SwitchProfile("job-1", 1, twoZoneProfile("heavy", 90, 110), 30, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := state.Start("job-1", 1, profile, 0, now.Add(2*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if len(state.Snapshot().Segments) != 2 {
		t.Fatal("restarting the same job must preserve existing segments")
	}
}
