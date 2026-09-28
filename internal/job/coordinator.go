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
	profiles    *dryer.Registry
	dryerCancel context.CancelFunc
	dryerDone   <-chan error
	tolerance   float64
	dwell       time.Duration
}

func NewCoordinator(state *State, dryerState *dryer.State, profiles *dryer.Registry) *Coordinator {
	return &Coordinator{state: state, dryer: dryerState, profiles: profiles, tolerance: dryer.DefaultTolerance, dwell: dryer.DefaultDwell}
}

// StartDryerRamp selects an approved profile for the job and heats every zone
// of its first segment from measured temperatures. It returns only after all
// zones have reached steady state or the context is cancelled.
func (coordinator *Coordinator) StartDryerRamp(parent context.Context, job model.Job, profileName string, startMeter float64, step time.Duration) (dryer.Profile, error) {
	coordinator.mu.Lock()
	profile, err := coordinator.profiles.Approved(profileName, time.Now())
	if err != nil {
		coordinator.mu.Unlock()
		return dryer.Profile{}, err
	}
	if current, ok := coordinator.dryer.OpenProfile(); ok && current != profile.Name {
		coordinator.mu.Unlock()
		return dryer.Profile{}, fmt.Errorf("dryer is already running profile %q; switch profiles instead of restarting", current)
	}
	if err := coordinator.dryer.Start(string(job.ID), job.Generation, profile, startMeter, time.Now()); err != nil {
		coordinator.mu.Unlock()
		return dryer.Profile{}, err
	}
	cancel, done := coordinator.launchLocked(parent, job, step)
	coordinator.mu.Unlock()

	if err := await(parent, done); err != nil {
		cancel()
		return dryer.Profile{}, err
	}
	return profile, nil
}

// SwitchDryerProfile closes the running segment at boundaryMeter, preserving
// it together with every earlier segment, and opens a new segment under the
// next approved profile. New zones inherit measured temperatures so heating
// continues from real readings. Returns once the new segment is steady.
func (coordinator *Coordinator) SwitchDryerProfile(parent context.Context, job model.Job, profileName string, boundaryMeter float64, step time.Duration) (dryer.Profile, error) {
	coordinator.mu.Lock()
	profile, err := coordinator.profiles.Approved(profileName, time.Now())
	if err != nil {
		coordinator.mu.Unlock()
		return dryer.Profile{}, err
	}
	// Stop the previous ramp before moving the segment boundary so an
	// in-flight measurement can only land on the old, about-to-close segment.
	coordinator.cancelRampLocked()
	if err := coordinator.dryer.SwitchProfile(string(job.ID), job.Generation, profile, boundaryMeter, time.Now()); err != nil {
		coordinator.mu.Unlock()
		return dryer.Profile{}, err
	}
	cancel, done := coordinator.launchLocked(parent, job, step)
	coordinator.mu.Unlock()

	if err := await(parent, done); err != nil {
		cancel()
		return dryer.Profile{}, err
	}
	return profile, nil
}

func (coordinator *Coordinator) launchLocked(parent context.Context, job model.Job, step time.Duration) (context.CancelFunc, <-chan error) {
	coordinator.cancelRampLocked()
	ctx, cancel := context.WithCancel(parent)
	done := dryer.RampZones(ctx, coordinator.dryer, string(job.ID), job.Generation, step, coordinator.tolerance, coordinator.dwell, func(zoneName string, value float64) error {
		return coordinator.dryer.Observe(string(job.ID), job.Generation, zoneName, value, time.Now())
	})
	coordinator.dryerCancel = cancel
	coordinator.dryerDone = done
	return cancel, done
}

// cancelRampLocked cancels the running ramp and waits for its measurements to
// stop so a following profile switch cannot interleave writes into the new
// segment.
func (coordinator *Coordinator) cancelRampLocked() {
	cancel := coordinator.dryerCancel
	done := coordinator.dryerDone
	if cancel == nil {
		return
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
	}
	coordinator.dryerCancel = nil
	coordinator.dryerDone = nil
}

func await(parent context.Context, done <-chan error) error {
	select {
	case <-parent.Done():
		return parent.Err()
	case err := <-done:
		return err
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

// DryerReady reports whether the open segment has held steady state.
func (coordinator *Coordinator) DryerReady(now time.Time) bool {
	return coordinator.dryer.OpenReady(coordinator.tolerance, coordinator.dwell, now)
}

// DryerSnapshot exposes the measured per-segment dryer state.
func (coordinator *Coordinator) DryerSnapshot() dryer.SnapshotData {
	return coordinator.dryer.Snapshot()
}

// Profiles exposes the approved profile registry.
func (coordinator *Coordinator) Profiles() *dryer.Registry {
	return coordinator.profiles
}
