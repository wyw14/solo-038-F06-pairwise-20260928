package dryer

import (
	"fmt"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

// ZoneProof is the measured drying evidence for one zone at release time.
type ZoneProof struct {
	Name     string  `json:"name"`
	Target   float64 `json:"target"`
	Measured float64 `json:"measured"`
	Airflow  float64 `json:"airflow"`
}

type IntervalProof struct {
	Proof       model.Proof `json:"proof"`
	Profile     string      `json:"profile"`
	Temperature float64     `json:"temperature"`
	Airflow     float64     `json:"airflow"`
	Zones       []ZoneProof `json:"zones"`
}

// BuildProof attests an interval as fully dried from the measured zone state.
// Every zone of the active profile must be steady; the aggregate temperature
// and airflow are the bottleneck (minimum) values across zones so the proof
// never claims more than the weakest zone delivered.
func BuildProof(job model.Job, interval model.Interval, profile string, zones []ZoneState, now time.Time) (IntervalProof, error) {
	if err := interval.Validate(); err != nil {
		return IntervalProof{}, err
	}
	if profile == "" || len(zones) == 0 {
		return IntervalProof{}, fmt.Errorf("dryer proof requires an active profile with zones")
	}
	entries := make([]ZoneProof, 0, len(zones))
	temperature := 0.0
	airflow := 0.0
	for index, zone := range zones {
		if !zone.Steady {
			return IntervalProof{}, fmt.Errorf("dryer zone %q has not reached steady state", zone.Name)
		}
		if zone.Measured <= 0 || zone.Airflow <= 0 {
			return IntervalProof{}, fmt.Errorf("dryer zone %q has no measured evidence", zone.Name)
		}
		if index == 0 || zone.Measured < temperature {
			temperature = zone.Measured
		}
		if index == 0 || zone.Airflow < airflow {
			airflow = zone.Airflow
		}
		entries = append(entries, ZoneProof{Name: zone.Name, Target: zone.Target, Measured: zone.Measured, Airflow: zone.Airflow})
	}
	proof := model.Proof{Kind: "dryer", Source: "dryer-zones", JobGeneration: job.Generation, RollGeneration: job.RollGeneration, ValidFrom: interval.StartMeter, ValidTo: interval.EndMeter, ObservedAt: now.UTC()}
	return IntervalProof{Proof: proof, Profile: profile, Temperature: temperature, Airflow: airflow, Zones: entries}, nil
}

func (proof IntervalProof) Covers(interval model.Interval) bool {
	return proof.Proof.JobGeneration == interval.JobGeneration && proof.Proof.RollGeneration == interval.RollGeneration && proof.Proof.ValidFrom <= interval.StartMeter && proof.Proof.ValidTo >= interval.EndMeter
}
