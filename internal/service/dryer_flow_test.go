package service

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/solo-038/internal/dryer"
)

func testRuntime(t *testing.T) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(Config{DataDir: t.TempDir(), FestoonCapacity: 120, RegisterGap: time.Second, RegisterTolerance: 2.0})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return runtime
}

func TestStartDryerRejectsUnapprovedProfile(t *testing.T) {
	runtime := testRuntime(t)
	err := runtime.ExecuteOperation(context.Background(), OperationInput{Kind: "start_dryer", DryerProfile: "experimental"})
	if err == nil {
		t.Fatal("an unregistered dryer profile must be rejected")
	}
}

func TestStartDryerUsesSelectedApprovedProfile(t *testing.T) {
	runtime := testRuntime(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := runtime.ExecuteOperation(ctx, OperationInput{Kind: "load_roll"}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ExecuteOperation(ctx, OperationInput{Kind: "start_dryer", DryerProfile: "production"}); err != nil {
		t.Fatalf("start dryer: %v", err)
	}
	if name, ok := runtime.dryer.OpenProfile(); !ok || name != "production" {
		t.Fatalf("dryer must run the selected profile, got %q ok=%v", name, ok)
	}
	if !runtime.jobControl.DryerReady(time.Now()) {
		t.Fatal("job must report every zone steady before proceeding")
	}
	// Feed paper and open a register window covering the dried meters.
	if err := runtime.ExecuteOperation(ctx, OperationInput{Kind: "feed_web"}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ExecuteOperation(ctx, OperationInput{Kind: "adjust_phase"}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ExecuteOperation(ctx, OperationInput{Kind: "release_interval"}); err != nil {
		t.Fatalf("fully dried interval must release: %v", err)
	}
}

func TestSwitchDryerProfileOperationPreservesBoundary(t *testing.T) {
	runtime := testRuntime(t)
	heavy := dryer.Profile{Name: "heavy-stock", Zones: []dryer.Zone{{Name: "entry", Temperature: 82, Airflow: 22}, {Name: "exit", Temperature: 112, Airflow: 26}}}
	if err := runtime.RegisterDryerProfile(heavy); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ApproveDryerProfile("heavy-stock", "shift-lead"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := runtime.ExecuteOperation(ctx, OperationInput{Kind: "load_roll"}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ExecuteOperation(ctx, OperationInput{Kind: "start_dryer", DryerProfile: "production"}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ExecuteOperation(ctx, OperationInput{Kind: "feed_web"}); err != nil {
		t.Fatal(err)
	}
	boundary := 1.0
	if err := runtime.ExecuteOperation(ctx, OperationInput{Kind: "switch_dryer_profile", DryerProfile: "heavy-stock", BoundaryMeter: boundary}); err != nil {
		t.Fatalf("switch profile: %v", err)
	}
	segments := runtime.dryer.Snapshot().Segments
	if len(segments) != 2 {
		t.Fatalf("switch must preserve the old segment, got %d", len(segments))
	}
	if segments[0].Profile != "production" || segments[0].EndMeter != boundary {
		t.Fatalf("old segment must close at boundary %v: %+v", boundary, segments[0])
	}
	if segments[1].Profile != "heavy-stock" || segments[1].StartMeter != boundary {
		t.Fatalf("new segment must open at boundary: %+v", segments[1])
	}
	if !runtime.jobControl.DryerReady(time.Now()) {
		t.Fatal("new segment must reach steady state before the operation returns")
	}
}

func TestReleaseFailsWhenZoneLeavesSteadyState(t *testing.T) {
	runtime := testRuntime(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := runtime.ExecuteOperation(ctx, OperationInput{Kind: "load_roll"}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ExecuteOperation(ctx, OperationInput{Kind: "start_dryer", DryerProfile: "production"}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ExecuteOperation(ctx, OperationInput{Kind: "feed_web"}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ExecuteOperation(ctx, OperationInput{Kind: "adjust_phase"}); err != nil {
		t.Fatal(err)
	}
	current, _ := runtime.CurrentJob()
	// A cold reading on one zone after dwell elapsed resets steady state.
	if err := runtime.dryer.Observe(string(current.ID), current.Generation, "exit", 12, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ExecuteOperation(ctx, OperationInput{Kind: "release_interval"}); err == nil {
		t.Fatal("interval must not release while a zone is below steady state")
	}
}
