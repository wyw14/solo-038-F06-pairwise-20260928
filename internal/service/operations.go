package service

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/wyw14/solo-038/internal/model"
	"github.com/wyw14/solo-038/internal/unwind"
)

type Operation struct {
	ID        model.OperationID `json:"id"`
	Kind      string            `json:"kind"`
	JobID     model.JobID       `json:"job_id"`
	Status    string            `json:"status"`
	CreatedAt time.Time         `json:"created_at"`
}

// OperationInput carries optional parameters for an operation. DryerProfile
// selects the approved drying curve; BoundaryMeter (>0) marks where a profile
// switch closes the current segment.
type OperationInput struct {
	Kind          string
	DryerProfile  string
	BoundaryMeter float64
}

func (runtime *Runtime) CreateOperation(ctx context.Context, input OperationInput) (Operation, error) {
	if input.Kind == "" {
		return Operation{}, fmt.Errorf("operation kind is required")
	}
	select {
	case <-ctx.Done():
		return Operation{}, ctx.Err()
	default:
	}
	jobValue, ok := runtime.jobs.Current()
	if !ok {
		return Operation{}, fmt.Errorf("there is no active job")
	}
	if err := runtime.ExecuteOperation(ctx, input); err != nil {
		return Operation{}, err
	}
	return Operation{ID: model.NewOperationID(), Kind: input.Kind, JobID: jobValue.ID, Status: "accepted", CreatedAt: time.Now().UTC()}, nil
}

func (runtime *Runtime) Equipment() []map[string]any {
	roll, generation, feedOwner, feedSpeed := runtime.unwind.Snapshot()
	plate, plateGeneration, color, phases := runtime.press.Snapshot()
	ready, folderSpeed, cutLength, fault, lots := runtime.folder.Snapshot()
	values := []map[string]any{
		{"name": "unwind", "roll": roll, "generation": generation, "owner": feedOwner, "speed": feedSpeed},
		{"name": "press", "plate": plate, "generation": plateGeneration, "color": color, "phases": phases},
		{"name": "dryer", "state": runtime.dryerControlStatus()},
		{"name": "folder", "ready": ready, "speed": folderSpeed, "cut_length": cutLength, "fault": fault, "lots": lots},
	}
	sort.Slice(values, func(left, right int) bool { return values[left]["name"].(string) < values[right]["name"].(string) })
	return values
}

func (runtime *Runtime) dryerControlStatus() map[string]any {
	status, errors := runtime.dryerControl.Status()
	snapshot := runtime.dryer.Snapshot()
	profiles := runtime.dryerProfiles.List()
	return map[string]any{"startup": status, "errors": errors, "job_id": snapshot.JobID, "generation": snapshot.JobGen, "profile": snapshot.Profile, "active": snapshot.Active, "segments": snapshot.Segments, "ready": runtime.jobControl.DryerReady(time.Now()), "profiles": profiles}
}

func (runtime *Runtime) LoadRoll(roll unwind.Roll) (uint64, error) {
	return runtime.loader.Load(roll)
}
