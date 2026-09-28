package api

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/solo-038/internal/dryer"
	"github.com/wyw14/solo-038/internal/service"
)

type operationRequest struct {
	Kind          string  `json:"kind"`
	DryerProfile  string  `json:"dryer_profile"`
	BoundaryMeter float64 `json:"boundary_meter"`
}

func (request operationRequest) input() service.OperationInput {
	return service.OperationInput{Kind: request.Kind, DryerProfile: request.DryerProfile, BoundaryMeter: request.BoundaryMeter}
}

func (handler *Handler) operations(writer http.ResponseWriter, request *http.Request) {
	current, ok := handler.runtime.CurrentJob()
	if !ok {
		writeJSON(writer, http.StatusOK, []service.Operation{})
		return
	}
	writeJSON(writer, http.StatusOK, []map[string]any{{"job_id": current.ID, "state": current.State, "generation": current.Generation}})
}

func (handler *Handler) createOperation(writer http.ResponseWriter, request *http.Request) {
	var input operationRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	operation, err := handler.runtime.CreateOperation(request.Context(), input.input())
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	writeJSON(writer, http.StatusAccepted, operation)
}

func (handler *Handler) cancelCurrent(writer http.ResponseWriter, request *http.Request) {
	cancellation, err := handler.runtime.CancelCurrent(request.Context())
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	writeJSON(writer, http.StatusOK, cancellation)
}

type profileRequest struct {
	Name  string       `json:"name"`
	Zones []dryer.Zone `json:"zones"`
}

type approvalRequest struct {
	ApprovedBy string `json:"approved_by"`
}

func (handler *Handler) dryerProfiles(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, handler.runtime.DryerProfiles())
}

func (handler *Handler) registerDryerProfile(writer http.ResponseWriter, request *http.Request) {
	var input profileRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	profile := dryer.Profile{Name: input.Name, Zones: input.Zones}
	if err := handler.runtime.RegisterDryerProfile(profile); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"name": profile.Name, "status": string(dryer.ApprovalPending)})
}

func (handler *Handler) approveDryerProfile(writer http.ResponseWriter, request *http.Request) {
	name := chi.URLParam(request, "name")
	var input approvalRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	if err := handler.runtime.ApproveDryerProfile(name, input.ApprovedBy); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	record, ok := handler.findProfile(name)
	if !ok {
		writeError(writer, http.StatusNotFound, fmt.Errorf("dryer profile %q not found", name))
		return
	}
	writeJSON(writer, http.StatusOK, record)
}

func (handler *Handler) findProfile(name string) (dryer.ApprovedProfile, bool) {
	for _, profile := range handler.runtime.DryerProfiles() {
		if profile.Profile.Name == name {
			return profile, true
		}
	}
	return dryer.ApprovedProfile{}, false
}
