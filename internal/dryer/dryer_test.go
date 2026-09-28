package dryer

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

func testProfile() Profile {
	return Profile{Name: "production", Zones: []Zone{{Name: "entry", Temperature: 80, Airflow: 20}, {Name: "exit", Temperature: 100, Airflow: 24}}}
}

func TestCatalogApproval(t *testing.T) {
	catalog := NewCatalog()
	if _, err := catalog.Approved("production"); err == nil {
		t.Fatal("unregistered profile must not be approved")
	}
	profile := testProfile()
	if err := catalog.Register(profile, time.Now()); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := catalog.Approved("production"); err == nil {
		t.Fatal("registered but unapproved profile must not be approved")
	}
	if err := catalog.Approve("production", time.Now()); err != nil {
		t.Fatalf("approve: %v", err)
	}
	approved, err := catalog.Approved("production")
	if err != nil {
		t.Fatalf("approved: %v", err)
	}
	if approved.Name != "production" || len(approved.Zones) != 2 {
		t.Fatalf("approved profile mismatch: %+v", approved)
	}
	if err := catalog.Register(profile, time.Now()); err == nil {
		t.Fatal("duplicate registration must fail")
	}
	defaults := DefaultCatalog(time.Now())
	for _, name := range []string{"production", "delicate", "newsprint"} {
		if _, err := defaults.Approved(name); err != nil {
			t.Fatalf("default catalog must have approved %q: %v", name, err)
		}
	}
}

func TestLedgerColdPaperIsNotDried(t *testing.T) {
	state := NewState()
	now := time.Now()
	if err := state.Start("job-1", 1, testProfile(), 0, now); err != nil {
		t.Fatalf("start: %v", err)
	}
	// Paper moves while the zones are still cold: no dried span may open.
	if err := state.Observe("job-1", 1, 5, now); err != nil {
		t.Fatalf("observe cold: %v", err)
	}
	if err := state.RangeFullyDried("job-1", 1, 0, 5, 5); err == nil {
		t.Fatal("cold paper must not be reported fully dried")
	}
	// Zones reach steady state; only paper observed from now on is dried.
	if err := state.MarkSteady("job-1", 1, "entry", now); err != nil {
		t.Fatalf("mark entry: %v", err)
	}
	if err := state.Observe("job-1", 1, 6, now); err != nil {
		t.Fatalf("observe partial steady: %v", err)
	}
	if err := state.RangeFullyDried("job-1", 1, 0, 6, 6); err == nil {
		t.Fatal("one unsteady zone must block the whole span")
	}
	if err := state.MarkSteady("job-1", 1, "exit", now); err != nil {
		t.Fatalf("mark exit: %v", err)
	}
	if err := state.Observe("job-1", 1, 6, now); err != nil {
		t.Fatalf("observe steady: %v", err)
	}
	if err := state.Observe("job-1", 1, 10, now); err != nil {
		t.Fatalf("observe advance: %v", err)
	}
	if err := state.RangeFullyDried("job-1", 1, 6, 10, 10); err != nil {
		t.Fatalf("steady span must be fully dried: %v", err)
	}
	if err := state.RangeFullyDried("job-1", 1, 5, 10, 10); err == nil {
		t.Fatal("span must not extend backwards over cold paper")
	}
}

func TestSwitchProfilePreservesSegmentBoundary(t *testing.T) {
	state := NewState()
	now := time.Now()
	if err := state.Start("job-1", 1, testProfile(), 0, now); err != nil {
		t.Fatalf("start: %v", err)
	}
	for _, zone := range []string{"entry", "exit"} {
		if err := state.MarkSteady("job-1", 1, zone, now); err != nil {
			t.Fatalf("mark %s: %v", zone, err)
		}
	}
	if err := state.Observe("job-1", 1, 0, now); err != nil {
		t.Fatalf("observe: %v", err)
	}
	if err := state.Observe("job-1", 1, 4, now); err != nil {
		t.Fatalf("observe: %v", err)
	}
	delicate := Profile{Name: "delicate", Zones: []Zone{{Name: "entry", Temperature: 60, Airflow: 14}, {Name: "middle", Temperature: 75, Airflow: 18}, {Name: "exit", Temperature: 85, Airflow: 20}}}
	if err := state.SwitchProfile("job-1", 1, delicate, 4, now); err != nil {
		t.Fatalf("switch: %v", err)
	}
	_, _, profile, zones, segments, active := state.Snapshot()
	if !active || profile != "delicate" {
		t.Fatalf("active profile after switch = %q", profile)
	}
	if len(segments) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(segments))
	}
	oldSegment := segments[0]
	if oldSegment.Profile != "production" || !oldSegment.Closed || oldSegment.StartMeter != 0 || oldSegment.EndMeter != 4 {
		t.Fatalf("old segment boundary not preserved: %+v", oldSegment)
	}
	if len(oldSegment.Dried) != 1 || oldSegment.Dried[0].From != 0 || oldSegment.Dried[0].To != 4 {
		t.Fatalf("old segment dried span not preserved: %+v", oldSegment.Dried)
	}
	newSegment := segments[1]
	if newSegment.Profile != "delicate" || newSegment.Closed || newSegment.StartMeter != 4 {
		t.Fatalf("new segment must open at the switch meter: %+v", newSegment)
	}
	// The new profile's zones must re-prove steady state; shared zones keep
	// their measured temperature but not the steady latch.
	steady := 0
	for _, zone := range zones {
		if zone.Steady {
			steady++
		}
	}
	if steady != 0 {
		t.Fatal("zones of the new profile must not start steady")
	}
	// The old segment stays dried; the new segment contributes nothing yet.
	if err := state.RangeFullyDried("job-1", 1, 0, 4, 4); err != nil {
		t.Fatalf("old segment must remain fully dried: %v", err)
	}
	if err := state.RangeFullyDried("job-1", 1, 0, 5, 5); err == nil {
		t.Fatal("range crossing into the unproven new segment must not be fully dried")
	}
	if err := state.SwitchProfile("job-1", 1, delicate, 4, now); err == nil {
		t.Fatal("switching to the already active profile must fail")
	}
}

func TestRampZonesSequentialSteady(t *testing.T) {
	state := NewState()
	profile := testProfile()
	if err := state.Start("job-1", 1, profile, 0, time.Now()); err != nil {
		t.Fatalf("start: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := RampZones(ctx, state, "job-1", 1, profile, time.Millisecond, func(zone string, temperature float64) error {
		return state.ReportMeasured("job-1", 1, zone, temperature, time.Now())
	})
	if err := <-done; err != nil {
		t.Fatalf("ramp: %v", err)
	}
	_, _, _, zones, _, _ := state.Snapshot()
	for _, zone := range zones {
		if !zone.Steady {
			t.Fatalf("zone %q must be steady after the ramp", zone.Name)
		}
		if zone.Measured <= 0 || zone.Measured < zone.Target-SteadyTolerance {
			t.Fatalf("zone %q measured %.2f below target %.2f", zone.Name, zone.Measured, zone.Target)
		}
	}
}

func TestBuildProofRequiresSteadyZones(t *testing.T) {
	job := model.Job{ID: model.NewJobID(), State: model.JobPrinting, Generation: 1, RollGeneration: 1}
	interval := model.Interval{JobID: job.ID, JobGeneration: 1, RollGeneration: 1, StartMeter: 2, EndMeter: 4, OpenedAt: time.Now(), ClosedAt: time.Now()}
	zones := []ZoneState{{Name: "entry", Target: 80, Measured: 79.5, Airflow: 20, Steady: true}, {Name: "exit", Target: 100, Measured: 99, Airflow: 24, Steady: false}}
	if _, err := BuildProof(job, interval, "production", zones, time.Now()); err == nil {
		t.Fatal("proof must fail while a zone is not steady")
	}
	zones[1].Steady = true
	proof, err := BuildProof(job, interval, "production", zones, time.Now())
	if err != nil {
		t.Fatalf("proof: %v", err)
	}
	if !proof.Covers(interval) {
		t.Fatal("proof must cover the interval")
	}
	if proof.Temperature != 79.5 || proof.Airflow != 20 {
		t.Fatalf("proof must report bottleneck values, got %.1f/%.1f", proof.Temperature, proof.Airflow)
	}
	if len(proof.Zones) != 2 || proof.Zones[0].Name != "entry" {
		t.Fatalf("proof must carry per-zone evidence: %+v", proof.Zones)
	}
	if !strings.Contains(proof.Proof.Source, "dryer") {
		t.Fatalf("proof source mismatch: %q", proof.Proof.Source)
	}
}
