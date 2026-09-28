package dryer

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type CatalogProfile struct {
	Profile   Profile   `json:"profile"`
	Approved  bool      `json:"approved"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Catalog struct {
	mu       sync.RWMutex
	profiles map[string]CatalogProfile
}

func NewCatalog() *Catalog {
	return &Catalog{profiles: map[string]CatalogProfile{}}
}

func DefaultCatalog(now time.Time) *Catalog {
	catalog := NewCatalog()
	profiles := []Profile{
		{Name: "production", Zones: []Zone{{Name: "entry", Temperature: 80, Airflow: 20}, {Name: "exit", Temperature: 100, Airflow: 24}}},
		{Name: "delicate", Zones: []Zone{{Name: "entry", Temperature: 60, Airflow: 14}, {Name: "middle", Temperature: 75, Airflow: 18}, {Name: "exit", Temperature: 85, Airflow: 20}}},
		{Name: "newsprint", Zones: []Zone{{Name: "entry", Temperature: 90, Airflow: 22}, {Name: "middle", Temperature: 110, Airflow: 28}, {Name: "exit", Temperature: 120, Airflow: 30}}},
	}
	for _, profile := range profiles {
		if err := catalog.Register(profile, now); err != nil {
			panic(fmt.Sprintf("default dryer profile %q is invalid: %v", profile.Name, err))
		}
		if err := catalog.Approve(profile.Name, now); err != nil {
			panic(fmt.Sprintf("default dryer profile %q cannot be approved: %v", profile.Name, err))
		}
	}
	return catalog
}

func (catalog *Catalog) Register(profile Profile, now time.Time) error {
	if err := profile.Validate(); err != nil {
		return err
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if _, ok := catalog.profiles[profile.Name]; ok {
		return fmt.Errorf("dryer profile %q is already registered", profile.Name)
	}
	catalog.profiles[profile.Name] = CatalogProfile{Profile: profile, Approved: false, UpdatedAt: now.UTC()}
	return nil
}

func (catalog *Catalog) Approve(name string, now time.Time) error {
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	entry, ok := catalog.profiles[name]
	if !ok {
		return fmt.Errorf("dryer profile %q is not registered", name)
	}
	entry.Approved = true
	entry.UpdatedAt = now.UTC()
	catalog.profiles[name] = entry
	return nil
}

func (catalog *Catalog) Approved(name string) (Profile, error) {
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	entry, ok := catalog.profiles[name]
	if !ok {
		return Profile{}, fmt.Errorf("dryer profile %q is not registered", name)
	}
	if !entry.Approved {
		return Profile{}, fmt.Errorf("dryer profile %q is not approved", name)
	}
	return entry.Profile, nil
}

func (catalog *Catalog) List() []CatalogProfile {
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	values := make([]CatalogProfile, 0, len(catalog.profiles))
	for _, entry := range catalog.profiles {
		values = append(values, entry)
	}
	sort.Slice(values, func(left, right int) bool { return values[left].Profile.Name < values[right].Profile.Name })
	return values
}
