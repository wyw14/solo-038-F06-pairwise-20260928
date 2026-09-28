package dryer

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

// ZoneProof is the measured steady-state evidence of one zone for a segment.
type ZoneProof struct {
	Name        string    `json:"name"`
	Temperature float64   `json:"temperature"`
	Airflow     float64   `json:"airflow"`
	Stable      bool      `json:"stable"`
	StableSince time.Time `json:"stable_since,omitempty"`
}

// SegmentProof attests that one meter sub-range (dried under one profile) was
// fully dried: every zone was measured at its target band and held steady
// state for the required dwell.
type SegmentProof struct {
	Proof      model.Proof `json:"proof"`
	Profile    string      `json:"profile"`
	StartMeter float64     `json:"start_meter"`
	EndMeter   float64     `json:"end_meter"`
	Zones      []ZoneProof `json:"zones"`
}

// Covers reports whether the proof belongs to the given generation and spans
// the supplied meter range.
func (proof SegmentProof) Covers(interval model.Interval) bool {
	return proof.Proof.JobGeneration == interval.JobGeneration && proof.Proof.RollGeneration == interval.RollGeneration && proof.Proof.ValidFrom <= interval.StartMeter && proof.Proof.ValidTo >= interval.EndMeter
}

// BuildSegmentProofs produces one dryer proof per profile segment overlapping
// the interval. If any covering segment has a zone that never reached steady
// state, the whole interval is rejected: the paper must not be recorded as
// fully dried. Segment boundaries from a profile switch are preserved.
func BuildSegmentProofs(job model.Job, interval model.Interval, state *State, tolerance float64, dwell time.Duration, now time.Time) ([]SegmentProof, error) {
	if err := interval.Validate(); err != nil {
		return nil, err
	}
	segments := state.SegmentsOverlapping(interval.StartMeter, interval.EndMeter)
	if len(segments) == 0 {
		return nil, fmt.Errorf("dryer has no segments covering meters %v..%v", interval.StartMeter, interval.EndMeter)
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].StartMeter < segments[j].StartMeter })
	if err := ensureCoverage(segments, interval.StartMeter, interval.EndMeter); err != nil {
		return nil, err
	}

	now = now.UTC()
	proofs := make([]SegmentProof, 0, len(segments))
	for _, segment := range segments {
		startMeter := math.Max(segment.StartMeter, interval.StartMeter)
		endMeter := interval.EndMeter
		if !segment.Open() && segment.EndMeter < endMeter {
			endMeter = segment.EndMeter
		}
		if !segment.Ready(tolerance, dwell, now) {
			return nil, fmt.Errorf("dryer segment meters %v..%v under profile %q is not at steady state", startMeter, endMeter, segment.Profile)
		}
		zoneProofs := make([]ZoneProof, 0, len(segment.Zones))
		for _, zone := range segment.Zones {
			if zone.Current <= 0 || zone.Airflow <= 0 {
				return nil, fmt.Errorf("dryer temperature and airflow proof are required for zone %q", zone.Name)
			}
			zoneProofs = append(zoneProofs, ZoneProof{Name: zone.Name, Temperature: zone.Current, Airflow: zone.Airflow, Stable: true, StableSince: zone.StableSince})
		}
		proof := model.Proof{Kind: "dryer", Source: "dryer-zones:" + segment.Profile, Profile: segment.Profile, JobGeneration: job.Generation, RollGeneration: job.RollGeneration, ValidFrom: startMeter, ValidTo: endMeter, ObservedAt: now}
		proofs = append(proofs, SegmentProof{Proof: proof, Profile: segment.Profile, StartMeter: startMeter, EndMeter: endMeter, Zones: zoneProofs})
	}
	return proofs, nil
}

func ensureCoverage(segments []Segment, startMeter, endMeter float64) error {
	cursor := startMeter
	for _, segment := range segments {
		if segment.StartMeter > cursor {
			return fmt.Errorf("dryer coverage gap from meter %v to %v", cursor, segment.StartMeter)
		}
		if segment.Open() {
			return nil
		}
		if segment.EndMeter >= endMeter {
			return nil
		}
		cursor = segment.EndMeter
	}
	return fmt.Errorf("dryer segments do not cover through meter %v", endMeter)
}

// Models converts segment proofs into the model proofs persisted in the journal.
func Models(proofs []SegmentProof) []model.Proof {
	values := make([]model.Proof, 0, len(proofs))
	for _, proof := range proofs {
		values = append(values, proof.Proof)
	}
	return values
}
