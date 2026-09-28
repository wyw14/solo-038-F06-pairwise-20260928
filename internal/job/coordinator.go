package job

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/solo-038/internal/dryer"
	"github.com/wyw14/solo-038/internal/model"
)

type Coordinator struct {
	mu          sync.Mutex
	state       *State
	dryer       *dryer.State
	dryerCancel context.CancelFunc
	dryerDone   <-chan error
}

func NewCoordinator(state *State, dryerState *dryer.State) *Coordinator {
	return &Coordinator{state: state, dryer: dryerState}
}

// StartDryerRamp starts heating the dryer for a job using an approved zone
// profile. The zones ramp sequentially from their measured temperatures. The
// ramp runs for the lifetime of the job run; it is stopped by Cancel or
// SwitchDryerProfile, not by the requesting HTTP call ending.
func (coordinator *Coordinator) StartDryerRamp(job model.Job, profile dryer.Profile, startMeter float64, interval time.Duration) error {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if err := coordinator.dryer.Start(string(job.ID), job.Generation, profile, startMeter, time.Now()); err != nil {
		return err
	}
	coordinator.startRampLocked(job, profile, interval)
	return nil
}

// SwitchDryerProfile moves the running job to a new approved profile. The old
// ramp is stopped, the segment boundary is recorded at meter, and the new
// profile's zones ramp from their current measured temperatures.
func (coordinator *Coordinator) SwitchDryerProfile(job model.Job, profile dryer.Profile, meter float64, interval time.Duration) error {
	coordinator.mu.Lock()
	cancel := coordinator.dryerCancel
	done := coordinator.dryerDone
	coordinator.dryerCancel = nil
	coordinator.dryerDone = nil
	coordinator.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(time.Second):
			return fmt.Errorf("dryer ramp did not stop")
		}
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if err := coordinator.dryer.SwitchProfile(string(job.ID), job.Generation, profile, meter, time.Now()); err != nil {
		return err
	}
	coordinator.startRampLocked(job, profile, interval)
	return nil
}

func (coordinator *Coordinator) startRampLocked(job model.Job, profile dryer.Profile, interval time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	coordinator.dryerCancel = cancel
	coordinator.dryerDone = dryer.RampZones(ctx, coordinator.dryer, string(job.ID), job.Generation, profile, interval, func(zone string, value float64) error {
		return coordinator.dryer.ReportMeasured(string(job.ID), job.Generation, zone, value, time.Now())
	})
}

// WaitDryer blocks until the active ramp has brought every zone to steady
// state or the context is done.
func (coordinator *Coordinator) WaitDryer(ctx context.Context) error {
	coordinator.mu.Lock()
	done := coordinator.dryerDone
	coordinator.mu.Unlock()
	if done == nil {
		return fmt.Errorf("dryer ramp has not started")
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (coordinator *Coordinator) Cancel(job model.Job, now time.Time) error {
	coordinator.mu.Lock()
	cancel := coordinator.dryerCancel
	done := coordinator.dryerDone
	coordinator.mu.Unlock()
	updated, err := job.Transition(model.JobCancelled, now)
	if err != nil {
		return err
	}
	if err := coordinator.state.Put(updated); err != nil {
		return err
	}
	if cancel != nil {
		cancel()
	}
	coordinator.dryer.Cancel(string(job.ID), job.Generation)
	if done != nil {
		select {
		case <-done:
		case <-time.After(time.Second):
			return fmt.Errorf("dryer ramp did not stop")
		}
	}
	return nil
}

// DryerTemperature reports the bottleneck (minimum) measured zone temperature.
func (coordinator *Coordinator) DryerTemperature() float64 {
	_, _, _, zones, _, _ := coordinator.dryer.Snapshot()
	if len(zones) == 0 {
		return 0
	}
	lowest := zones[0].Measured
	for _, zone := range zones[1:] {
		if zone.Measured < lowest {
			lowest = zone.Measured
		}
	}
	return lowest
}
