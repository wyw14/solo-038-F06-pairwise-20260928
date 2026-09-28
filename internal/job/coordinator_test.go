package job

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/solo-038/internal/dryer"
	"github.com/wyw14/solo-038/internal/model"
)

func newTestJob() model.Job {
	current := model.NewJob(time.Now())
	current.RollGeneration = 1
	return current
}

func TestStartDryerRampRequiresApprovedProfile(t *testing.T) {
	coordinator := NewCoordinator(NewState(), dryer.NewState(), dryer.NewRegistry())
	current := newTestJob()
	if _, err := coordinator.StartDryerRamp(context.Background(), current, "production", 0, time.Millisecond); err == nil {
		t.Fatal("an unregistered profile must be rejected")
	}
}

func TestStartDryerRampReachesPerZoneSteadyState(t *testing.T) {
	coordinator := NewCoordinator(NewState(), dryer.NewState(), dryer.DefaultRegistry())
	current := newTestJob()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	profile, err := coordinator.StartDryerRamp(ctx, current, "production", 0, time.Millisecond)
	if err != nil {
		t.Fatalf("start ramp: %v", err)
	}
	if profile.Name != "production" {
		t.Fatalf("selected profile = %q", profile.Name)
	}
	if !coordinator.DryerReady(time.Now()) {
		t.Fatal("dryer must report steady state after ramp")
	}
	snapshot := coordinator.DryerSnapshot()
	if len(snapshot.Segments) != 1 || snapshot.Segments[0].StartMeter != 0 {
		t.Fatalf("unexpected segments: %+v", snapshot.Segments)
	}
}

func TestSwitchDryerProfileKeepsBoundaryBetweenSegments(t *testing.T) {
	dryerState := dryer.NewState()
	registry := dryer.DefaultRegistry()
	heavy := dryer.Profile{Name: "heavy-stock", Zones: []dryer.Zone{{Name: "entry", Temperature: 85, Airflow: 22}, {Name: "exit", Temperature: 115, Airflow: 26}}}
	if err := registry.Register(heavy); err != nil {
		t.Fatal(err)
	}
	if err := registry.Approve("heavy-stock", "engineer", time.Now()); err != nil {
		t.Fatal(err)
	}
	coordinator := NewCoordinator(NewState(), dryerState, registry)
	current := newTestJob()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := coordinator.StartDryerRamp(ctx, current, "production", 0, time.Millisecond); err != nil {
		t.Fatalf("start ramp: %v", err)
	}
	boundary := 75.0
	if _, err := coordinator.SwitchDryerProfile(ctx, current, "heavy-stock", boundary, time.Millisecond); err != nil {
		t.Fatalf("switch profile: %v", err)
	}
	if !coordinator.DryerReady(time.Now()) {
		t.Fatal("new profile segment must reach steady state")
	}
	segments := coordinator.DryerSnapshot().Segments
	if len(segments) != 2 {
		t.Fatalf("want old and new segments preserved, got %d", len(segments))
	}
	if segments[0].Profile != "production" || segments[0].EndMeter != boundary {
		t.Fatalf("old segment must close at boundary: %+v", segments[0])
	}
	if segments[1].Profile != "heavy-stock" || segments[1].StartMeter != boundary || !segments[1].Open() {
		t.Fatalf("new segment must open at boundary: %+v", segments[1])
	}
	// Heating on the new segment resumes from inherited measured readings,
	// never from zero.
	for _, zone := range segments[1].Zones {
		if zone.Current <= 0 {
			t.Fatalf("zone %s must resume from a measured temperature", zone.Name)
		}
	}
}

func TestSwitchToSameProfileRejected(t *testing.T) {
	coordinator := NewCoordinator(NewState(), dryer.NewState(), dryer.DefaultRegistry())
	current := newTestJob()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := coordinator.StartDryerRamp(ctx, current, "production", 0, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.SwitchDryerProfile(ctx, current, "production", 10, time.Millisecond); err == nil {
		t.Fatal("switching to the already-running profile must be rejected")
	}
}
