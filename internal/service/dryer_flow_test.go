package service

import (
	"context"
	"strings"
	"testing"
	"time"
)

func newTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(Config{DataDir: t.TempDir(), FestoonCapacity: 120, RegisterGap: 500 * time.Millisecond, RegisterTolerance: 0.25})
	if err != nil {
		t.Fatalf("runtime: %v", err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	return runtime
}

func mustOperate(t *testing.T, runtime *Runtime, kind string, dryerProfile string) {
	t.Helper()
	if _, err := runtime.CreateOperation(context.Background(), kind, dryerProfile); err != nil {
		t.Fatalf("operation %s: %v", kind, err)
	}
}

func waitDryerSteady(t *testing.T, runtime *Runtime) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.DryerSteady() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("dryer zones did not reach steady state")
}

func TestStartDryerUsesSelectedApprovedProfile(t *testing.T) {
	runtime := newTestRuntime(t)
	if _, err := runtime.CreateOperation(context.Background(), "start_dryer", "experimental"); err == nil {
		t.Fatal("unapproved profile must be rejected")
	}
	mustOperate(t, runtime, "start_dryer", "delicate")
	waitDryerSteady(t, runtime)
	_, _, profile, zones, _, active := runtime.dryer.Snapshot()
	if !active || profile != "delicate" {
		t.Fatalf("dryer must run the selected profile, got %q", profile)
	}
	if len(zones) != 3 {
		t.Fatalf("delicate profile must have 3 zones, got %d", len(zones))
	}
	for _, zone := range zones {
		if !zone.Steady {
			t.Fatalf("zone %q must be steady", zone.Name)
		}
		if zone.Measured < zone.Target-2 {
			t.Fatalf("zone %q measured %.1f below target %.1f", zone.Name, zone.Measured, zone.Target)
		}
	}
}

func TestReleaseRequiresSteadyDrying(t *testing.T) {
	runtime := newTestRuntime(t)
	mustOperate(t, runtime, "load_roll", "")
	mustOperate(t, runtime, "feed_web", "")
	mustOperate(t, runtime, "start_dryer", "delicate")
	mustOperate(t, runtime, "adjust_phase", "")
	if _, err := runtime.CreateOperation(context.Background(), "release_interval", ""); err == nil {
		t.Fatal("release must fail while the zones have not reached steady state")
	} else if !strings.Contains(err.Error(), "steady") && !strings.Contains(err.Error(), "dried") {
		t.Fatalf("release failure must mention drying, got: %v", err)
	}
	waitDryerSteady(t, runtime)
	mustOperate(t, runtime, "feed_web", "")
	mustOperate(t, runtime, "adjust_phase", "")
	mustOperate(t, runtime, "feed_web", "")
	mustOperate(t, runtime, "release_interval", "")
}

func TestSwitchDryerProfilePreservesBoundaries(t *testing.T) {
	runtime := newTestRuntime(t)
	mustOperate(t, runtime, "load_roll", "")
	mustOperate(t, runtime, "feed_web", "")
	mustOperate(t, runtime, "start_dryer", "production")
	waitDryerSteady(t, runtime)
	mustOperate(t, runtime, "feed_web", "")
	mustOperate(t, runtime, "feed_web", "")
	if _, err := runtime.CreateOperation(context.Background(), "switch_dryer_profile", "unknown"); err == nil {
		t.Fatal("switching to an unapproved profile must fail")
	}
	mustOperate(t, runtime, "switch_dryer_profile", "delicate")
	_, _, profile, _, segments, _ := runtime.dryer.Snapshot()
	if profile != "delicate" {
		t.Fatalf("active profile after switch = %q", profile)
	}
	if len(segments) != 2 {
		t.Fatalf("switch must keep both segments, got %d", len(segments))
	}
	oldSegment := segments[0]
	if oldSegment.Profile != "production" || !oldSegment.Closed || oldSegment.EndMeter != 3 {
		t.Fatalf("old segment boundary not preserved: %+v", oldSegment)
	}
	if segments[1].Profile != "delicate" || segments[1].StartMeter != 3 || segments[1].Closed {
		t.Fatalf("new segment must open at the switch meter: %+v", segments[1])
	}
	// Paper dried under the old profile stays dried; the new profile must
	// re-prove steady state before its paper can be released.
	mustOperate(t, runtime, "adjust_phase", "")
	mustOperate(t, runtime, "feed_web", "")
	if _, err := runtime.CreateOperation(context.Background(), "release_interval", ""); err == nil {
		t.Fatal("release must fail until the new profile's zones are steady")
	}
	waitDryerSteady(t, runtime)
	mustOperate(t, runtime, "feed_web", "")
	mustOperate(t, runtime, "adjust_phase", "")
	mustOperate(t, runtime, "feed_web", "")
	mustOperate(t, runtime, "release_interval", "")
}
