package service

import (
	"fmt"
	"time"

	"github.com/wyw14/solo-038/internal/dryer"
)

// DryerProfiles lists all registered profiles with their approval status.
func (runtime *Runtime) DryerProfiles() []dryer.ApprovedProfile {
	return runtime.dryerProfiles.List()
}

// RegisterDryerProfile registers (or replaces) a draft profile. Replacing an
// approved profile with different zones revokes its approval.
func (runtime *Runtime) RegisterDryerProfile(profile dryer.Profile) error {
	if err := profile.Validate(); err != nil {
		return err
	}
	return runtime.dryerProfiles.Register(profile)
}

// ApproveDryerProfile signs off a registered profile so jobs may select it.
func (runtime *Runtime) ApproveDryerProfile(name, approvedBy string) error {
	if name == "" || approvedBy == "" {
		return fmt.Errorf("profile name and approver are required")
	}
	return runtime.dryerProfiles.Approve(name, approvedBy, time.Now())
}
