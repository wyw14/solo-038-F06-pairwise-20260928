package dryer

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

func steadyJob(t *testing.T) (model.Job, *State, time.Time) {
	t.Helper()
	now := time.Now()
	job := model.NewJob(now)
	job.RollGeneration = 1
	state := NewState()
	if err := state.Start(string(job.ID), job.Generation, twoZoneProfile("production", 80, 100), 0, now); err != nil {
		t.Fatal(err)
	}
	for _, zone := range state.Snapshot().Segments[0].Zones {
		if err := state.Observe(string(job.ID), job.Generation, zone.Name, zone.Target, now); err != nil {
			t.Fatal(err)
		}
	}
	return job, state, now
}

func interval(job model.Job, from, to float64, now time.Time) model.Interval {
	return model.Interval{JobID: job.ID, JobGeneration: job.Generation, RollGeneration: job.RollGeneration, StartMeter: from, EndMeter: to, OpenedAt: now, ClosedAt: now.Add(time.Millisecond)}
}

func TestBuildSegmentProofsForSingleSteadySegment(t *testing.T) {
	job, state, now := steadyJob(t)
	later := now.Add(100 * time.Millisecond)
	proofs, err := BuildSegmentProofs(job, interval(job, 0, 100, later), state, 0.5, 20*time.Millisecond, later)
	if err != nil {
		t.Fatalf("build proofs: %v", err)
	}
	if len(proofs) != 1 {
		t.Fatalf("want 1 proof, got %d", len(proofs))
	}
	set := model.ProofSet{Interval: interval(job, 0, 100, later), Proofs: Models(proofs)}
	if !set.Complete("dryer") {
		t.Fatal("steady segment must completely cover the interval")
	}
	if proofs[0].Profile != "production" || proofs[0].Proof.Source != "dryer-zones:production" {
		t.Fatalf("proof must name its profile, got %+v", proofs[0])
	}
	for _, zone := range proofs[0].Zones {
		if !zone.Stable || zone.Temperature <= 0 || zone.Airflow <= 0 {
			t.Fatalf("zone proof must carry stable measured evidence: %+v", zone)
		}
	}
}

func TestProofsJoinAcrossProfileSwitchBoundary(t *testing.T) {
	job, state, now := steadyJob(t)
	boundary := 120.0
	second := twoZoneProfile("heavy-stock", 80, 120)
	switchAt := now.Add(50 * time.Millisecond)
	if err := state.SwitchProfile(string(job.ID), job.Generation, second, boundary, switchAt); err != nil {
		t.Fatal(err)
	}
	steadyAt := switchAt.Add(50 * time.Millisecond)
	for _, zone := range state.Snapshot().Segments[1].Zones {
		if err := state.Observe(string(job.ID), job.Generation, zone.Name, zone.Target, switchAt.Add(time.Millisecond)); err != nil {
			t.Fatal(err)
		}
	}
	proofs, err := BuildSegmentProofs(job, interval(job, 20, 220, steadyAt), state, 0.5, 20*time.Millisecond, steadyAt)
	if err != nil {
		t.Fatalf("build proofs across switch: %v", err)
	}
	if len(proofs) != 2 {
		t.Fatalf("want one proof per segment, got %d", len(proofs))
	}
	if proofs[0].StartMeter != 20 || proofs[0].EndMeter != boundary {
		t.Fatalf("old proof clipped to 20..%v, got %v..%v", boundary, proofs[0].StartMeter, proofs[0].EndMeter)
	}
	if proofs[1].StartMeter != boundary || proofs[1].EndMeter != 220 {
		t.Fatalf("new proof clipped to %v..220, got %v..%v", boundary, proofs[1].StartMeter, proofs[1].EndMeter)
	}
	set := model.ProofSet{Interval: interval(job, 20, 220, steadyAt), Proofs: Models(proofs)}
	if !set.Complete("dryer") {
		t.Fatal("two segment proofs must jointly cover the switched interval")
	}
	sources := set.Sources()
	if len(sources) != 2 {
		t.Fatalf("want 2 distinct profile sources, got %v", sources)
	}
}

func TestUnsteadyZoneBlocksWholeInterval(t *testing.T) {
	job, state, now := steadyJob(t)
	boundary := 120.0
	second := twoZoneProfile("heavy-stock", 90, 120)
	switchAt := now.Add(50 * time.Millisecond)
	if err := state.SwitchProfile(string(job.ID), job.Generation, second, boundary, switchAt); err != nil {
		t.Fatal(err)
	}
	// Only the entry zone reaches target; the exit zone stays cold.
	if err := state.Observe(string(job.ID), job.Generation, "entry", 90, switchAt.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	later := switchAt.Add(100 * time.Millisecond)
	_, err := BuildSegmentProofs(job, interval(job, 0, 220, later), state, 0.5, 20*time.Millisecond, later)
	if err == nil {
		t.Fatal("interval with an unsteady zone must not be certified fully dried")
	}
	if !strings.Contains(err.Error(), "steady state") {
		t.Fatalf("error must explain the steady-state failure, got: %v", err)
	}
	// A release confined to the fully-dried old segment is still valid.
	proofs, err := BuildSegmentProofs(job, interval(job, 0, boundary, later), state, 0.5, 20*time.Millisecond, later)
	if err != nil {
		t.Fatalf("old segment should still be certifiable: %v", err)
	}
	if len(proofs) != 1 || proofs[0].Profile != "production" {
		t.Fatalf("only the old segment should be proved, got %+v", proofs)
	}
}

func TestCoverageGapRejectsProofs(t *testing.T) {
	job, state, now := steadyJob(t)
	// Interval starting before any segment cannot be covered.
	before := interval(job, -10, 50, now.Add(50*time.Millisecond))
	if _, err := BuildSegmentProofs(job, before, state, 0.5, 20*time.Millisecond, now.Add(50*time.Millisecond)); err == nil {
		t.Fatal("interval before the first segment must fail coverage")
	}
}

func TestProofSetCompleteRequiresJointCoverage(t *testing.T) {
	job := model.NewJob(time.Now())
	job.RollGeneration = 1
	span := interval(job, 0, 100, time.Now())
	partial := []model.Proof{
		{Kind: "dryer", JobGeneration: job.Generation, RollGeneration: job.RollGeneration, ValidFrom: 0, ValidTo: 40},
		{Kind: "dryer", JobGeneration: job.Generation, RollGeneration: job.RollGeneration, ValidFrom: 60, ValidTo: 100},
	}
	if (model.ProofSet{Interval: span, Proofs: partial}).Complete("dryer") {
		t.Fatal("proofs with a 40..60 gap must not complete the interval")
	}
	joined := append(partial, model.Proof{Kind: "dryer", JobGeneration: job.Generation, RollGeneration: job.RollGeneration, ValidFrom: 40, ValidTo: 60})
	if !(model.ProofSet{Interval: span, Proofs: joined}).Complete("dryer") {
		t.Fatal("adjacent proofs meeting at the boundary must complete the interval")
	}
	wrongGeneration := []model.Proof{{Kind: "dryer", JobGeneration: job.Generation + 1, RollGeneration: job.RollGeneration, ValidFrom: 0, ValidTo: 100}}
	if (model.ProofSet{Interval: span, Proofs: wrongGeneration}).Complete("dryer") {
		t.Fatal("proofs from another generation must not count")
	}
}

func TestSegmentMarshalOpenEndMeterAsNull(t *testing.T) {
	segment := Segment{Profile: "p", StartMeter: 0, EndMeter: math.Inf(1), Zones: []ZoneState{{Name: "z", Target: 80}}}
	encoded, err := json.Marshal(segment)
	if err != nil {
		t.Fatalf("marshal open segment: %v", err)
	}
	if !strings.Contains(string(encoded), `"end_meter":null`) {
		t.Fatalf("open end meter must serialize as null, got %s", encoded)
	}
}
