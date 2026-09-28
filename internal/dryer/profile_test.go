package dryer

import (
	"testing"
	"time"
)

func TestRegistryOnlyApprovedProfilesAreSelectable(t *testing.T) {
	registry := NewRegistry()
	draft := Profile{Name: "experimental", Zones: []Zone{{Name: "entry", Temperature: 70, Airflow: 18}}}
	if err := registry.Register(draft); err != nil {
		t.Fatalf("register draft: %v", err)
	}
	now := time.Now()
	if _, err := registry.Approved("experimental", now); err == nil {
		t.Fatal("draft profile must not be selectable as approved")
	}
	if err := registry.Approve("experimental", "process-engineer", now); err != nil {
		t.Fatalf("approve: %v", err)
	}
	selected, err := registry.Approved("experimental", now.Add(time.Second))
	if err != nil {
		t.Fatalf("approved profile should be selectable: %v", err)
	}
	if selected.Name != "experimental" {
		t.Fatalf("unexpected profile %q", selected.Name)
	}
	if _, err := registry.Approved("missing", now); err == nil {
		t.Fatal("unknown profile must not be selectable")
	}
}

func TestRegistryChangingApprovedProfileRevokesApproval(t *testing.T) {
	registry := NewRegistry()
	profile := Profile{Name: "production", Zones: []Zone{{Name: "entry", Temperature: 80, Airflow: 20}}}
	if err := registry.Register(profile); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := registry.Approve("production", "engineer", now); err != nil {
		t.Fatal(err)
	}
	profile.Zones[0].Temperature = 95
	if err := registry.Register(profile); err != nil {
		t.Fatal(err)
	}
	if record, ok := registry.Get("production"); !ok || record.Status != ApprovalPending {
		t.Fatalf("modified profile must be pending again, got %+v", record)
	}
	if _, err := registry.Approved("production", now.Add(time.Second)); err == nil {
		t.Fatal("modified profile must require renewed approval")
	}
}

func TestRegistryRejectsInvalidApprovalIdentity(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register(Profile{Name: "p", Zones: []Zone{{Name: "z", Temperature: 1, Airflow: 1}}}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Approve("p", "", time.Now()); err == nil {
		t.Fatal("approval without approver must fail")
	}
}
