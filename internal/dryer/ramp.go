package dryer

import (
	"context"
	"fmt"
	"math"
	"time"
)

const (
	// SteadyTolerance is the band around a zone setpoint that counts as steady.
	SteadyTolerance = 1.5
	// SteadyDwell is how many consecutive ticks inside the band latch steady.
	SteadyDwell = 2
)

// RampZones heats the zones of profile one after another, in profile order.
// Each zone is driven from its measured temperature toward its setpoint and
// the next zone only starts once the current one has held inside
// SteadyTolerance for SteadyDwell consecutive ticks. The returned channel
// receives the terminal error (nil on success or cancellation) and is closed.
func RampZones(ctx context.Context, state *State, jobID string, generation uint64, profile Profile, step time.Duration, apply func(zone string, temperature float64) error) <-chan error {
	result := make(chan error, 1)
	go func() {
		defer close(result)
		if step <= 0 {
			step = 10 * time.Millisecond
		}
		ticker := time.NewTicker(step)
		defer ticker.Stop()
		for _, zone := range profile.Zones {
			dwell := 0
			for {
				select {
				case <-ctx.Done():
					result <- nil
					return
				case <-ticker.C:
				}
				_, _, activeProfile, zones, _, active := state.Snapshot()
				if !active {
					result <- fmt.Errorf("dryer ramp stopped: dryer is not active")
					return
				}
				if activeProfile != profile.Name {
					// Superseded by SwitchDryerProfile; the new profile has
					// its own ramp.
					result <- nil
					return
				}
				measured := 0.0
				found := false
				for _, current := range zones {
					if current.Name == zone.Name {
						measured = current.Measured
						found = true
					}
				}
				if !found {
					result <- fmt.Errorf("dryer zone %q is not in the active profile", zone.Name)
					return
				}
				next := measured + (zone.Temperature-measured)*0.2
				if err := apply(zone.Name, next); err != nil {
					result <- err
					return
				}
				if math.Abs(next-zone.Temperature) <= SteadyTolerance {
					dwell++
				} else {
					dwell = 0
				}
				if dwell >= SteadyDwell {
					if err := state.MarkSteady(jobID, generation, zone.Name, time.Now()); err != nil {
						result <- err
						return
					}
					break
				}
			}
		}
		result <- nil
	}()
	return result
}
