package model

import (
	"errors"
	"sort"
	"time"
)

type Interval struct {
	JobID          JobID     `json:"job_id"`
	JobGeneration  uint64    `json:"job_generation"`
	RollGeneration uint64    `json:"roll_generation"`
	StartMeter     float64   `json:"start_meter"`
	EndMeter       float64   `json:"end_meter"`
	OpenedAt       time.Time `json:"opened_at"`
	ClosedAt       time.Time `json:"closed_at"`
}

type Proof struct {
	Kind           string    `json:"kind"`
	Source         string    `json:"source"`
	Profile        string    `json:"profile,omitempty"`
	JobGeneration  uint64    `json:"job_generation"`
	RollGeneration uint64    `json:"roll_generation"`
	ValidFrom      float64   `json:"valid_from"`
	ValidTo        float64   `json:"valid_to"`
	ObservedAt     time.Time `json:"observed_at"`
}

type ProofSet struct {
	Interval Interval `json:"interval"`
	Proofs   []Proof  `json:"proofs"`
}

func (interval Interval) Validate() error {
	if !interval.JobID.Valid() || interval.JobGeneration == 0 || interval.RollGeneration == 0 {
		return errors.New("interval identity is incomplete")
	}
	if interval.EndMeter <= interval.StartMeter {
		return errors.New("interval meters are invalid")
	}
	if interval.ClosedAt.Before(interval.OpenedAt) {
		return errors.New("interval time range is invalid")
	}
	return nil
}

func (set ProofSet) Complete(required ...string) bool {
	for _, kind := range required {
		if !set.CoversKind(kind) {
			return false
		}
	}
	return true
}

// CoversKind reports whether proofs of one kind jointly span the whole
// interval without gaps, preserving boundaries between adjacent proofs.
func (set ProofSet) CoversKind(kind string) bool {
	type span struct{ from, to float64 }
	spans := []span{}
	for _, proof := range set.Proofs {
		if proof.Kind != kind || proof.JobGeneration != set.Interval.JobGeneration || proof.RollGeneration != set.Interval.RollGeneration {
			continue
		}
		if proof.ValidFrom > set.Interval.EndMeter || proof.ValidTo < set.Interval.StartMeter {
			continue
		}
		from := proof.ValidFrom
		if from < set.Interval.StartMeter {
			from = set.Interval.StartMeter
		}
		to := proof.ValidTo
		if to > set.Interval.EndMeter {
			to = set.Interval.EndMeter
		}
		spans = append(spans, span{from: from, to: to})
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].from < spans[j].from })
	cursor := set.Interval.StartMeter
	for _, value := range spans {
		if value.from > cursor {
			return false
		}
		if value.to > cursor {
			cursor = value.to
		}
		if cursor >= set.Interval.EndMeter {
			return true
		}
	}
	return false
}

func (set ProofSet) Sources() []string {
	seen := map[string]bool{}
	for _, proof := range set.Proofs {
		seen[proof.Source] = true
	}
	values := make([]string, 0, len(seen))
	for source := range seen {
		values = append(values, source)
	}
	sort.Strings(values)
	return values
}
