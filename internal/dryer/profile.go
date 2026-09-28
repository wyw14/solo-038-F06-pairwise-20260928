package dryer

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type Zone struct {
	Name        string  `json:"name"`
	Temperature float64 `json:"temperature"`
	Airflow     float64 `json:"airflow"`
}

type Profile struct {
	Name  string `json:"name"`
	Zones []Zone `json:"zones"`
}

func (profile Profile) Validate() error {
	if profile.Name == "" || len(profile.Zones) == 0 {
		return fmt.Errorf("dryer profile name and zones are required")
	}
	seen := map[string]bool{}
	for _, zone := range profile.Zones {
		if zone.Name == "" || zone.Temperature < 0 || zone.Airflow < 0 || seen[zone.Name] {
			return fmt.Errorf("dryer profile zone is invalid")
		}
		seen[zone.Name] = true
	}
	return nil
}

func (profile Profile) MaximumTemperature() float64 {
	values := make([]float64, 0, len(profile.Zones))
	for _, zone := range profile.Zones {
		values = append(values, zone.Temperature)
	}
	sort.Float64s(values)
	if len(values) == 0 {
		return 0
	}
	return values[len(values)-1]
}

func (profile Profile) AverageAirflow() float64 {
	if len(profile.Zones) == 0 {
		return 0
	}
	total := 0.0
	for _, zone := range profile.Zones {
		total += zone.Airflow
	}
	return total / float64(len(profile.Zones))
}

// ApprovalStatus marks whether a profile may be selected by a job.
type ApprovalStatus string

const (
	ApprovalPending ApprovalStatus = "pending"
	ApprovalGranted ApprovalStatus = "approved"
)

// ApprovedProfile is a profile together with its approval record.
type ApprovedProfile struct {
	Profile    Profile        `json:"profile"`
	Status     ApprovalStatus `json:"status"`
	ApprovedBy string         `json:"approved_by,omitempty"`
	ApprovedAt time.Time      `json:"approved_at,omitempty"`
}

func (record ApprovedProfile) IsApproved(now time.Time) bool {
	return record.Status == ApprovalGranted && record.Profile.Validate() == nil && !record.ApprovedAt.IsZero() && !record.ApprovedAt.After(now.UTC())
}

// Registry holds the dryer profiles that have been defined for the line.
// Only approved profiles may be attached to a running job.
type Registry struct {
	mu       sync.RWMutex
	profiles map[string]ApprovedProfile
}

func NewRegistry() *Registry {
	return &Registry{profiles: map[string]ApprovedProfile{}}
}

// DefaultRegistry seeds the built-in production profile so existing jobs keep
// a selectable, approved curve instead of an implicit two-zone constant.
func DefaultRegistry() *Registry {
	registry := NewRegistry()
	production := Profile{Name: "production", Zones: []Zone{{Name: "entry", Temperature: 80, Airflow: 20}, {Name: "exit", Temperature: 100, Airflow: 24}}}
	if err := registry.Register(production); err != nil {
		panic(err)
	}
	if err := registry.Approve("production", "process-engineer", time.Unix(0, 0).UTC()); err != nil {
		panic(err)
	}
	return registry
}

// Register adds or replaces a draft profile. Replacing an approved profile
// drops the approval so a modified curve must be signed off again.
func (registry *Registry) Register(profile Profile) error {
	if err := profile.Validate(); err != nil {
		return err
	}
	// Copy the zone slice so later caller mutations cannot silently change
	// the stored (possibly approved) profile.
	zones := append([]Zone(nil), profile.Zones...)
	profile.Zones = zones
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if existing, ok := registry.profiles[profile.Name]; ok {
		if !sameProfile(existing.Profile, profile) {
			registry.profiles[profile.Name] = ApprovedProfile{Profile: profile, Status: ApprovalPending}
			return nil
		}
		return nil
	}
	registry.profiles[profile.Name] = ApprovedProfile{Profile: profile, Status: ApprovalPending}
	return nil
}

func (registry *Registry) Approve(name, approvedBy string, now time.Time) error {
	if name == "" || approvedBy == "" {
		return fmt.Errorf("profile approval identity is incomplete")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	record, ok := registry.profiles[name]
	if !ok {
		return fmt.Errorf("dryer profile %q is not registered", name)
	}
	if err := record.Profile.Validate(); err != nil {
		return err
	}
	record.Status = ApprovalGranted
	record.ApprovedBy = approvedBy
	record.ApprovedAt = now.UTC()
	registry.profiles[name] = record
	return nil
}

func (registry *Registry) Get(name string) (ApprovedProfile, bool) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	record, ok := registry.profiles[name]
	return record, ok
}

// Approved returns a validated, approved profile. Jobs must use this accessor
// so a draft or tampered curve can never reach the dryer.
func (registry *Registry) Approved(name string, now time.Time) (Profile, error) {
	record, ok := registry.Get(name)
	if !ok {
		return Profile{}, fmt.Errorf("dryer profile %q is not registered", name)
	}
	if !record.IsApproved(now) {
		return Profile{}, fmt.Errorf("dryer profile %q is not approved", name)
	}
	return record.Profile, nil
}

func (registry *Registry) List() []ApprovedProfile {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	names := make([]string, 0, len(registry.profiles))
	for name := range registry.profiles {
		names = append(names, name)
	}
	sort.Strings(names)
	values := make([]ApprovedProfile, 0, len(names))
	for _, name := range names {
		values = append(values, registry.profiles[name])
	}
	return values
}

func sameProfile(left, right Profile) bool {
	if left.Name != right.Name || len(left.Zones) != len(right.Zones) {
		return false
	}
	for index := range left.Zones {
		if left.Zones[index] != right.Zones[index] {
			return false
		}
	}
	return true
}
