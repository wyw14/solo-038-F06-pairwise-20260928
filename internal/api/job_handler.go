package api

import (
	"net/http"

	"github.com/wyw14/solo-038/internal/service"
)

type operationRequest struct {
	Kind         string `json:"kind"`
	DryerProfile string `json:"dryer_profile"`
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
	operation, err := handler.runtime.CreateOperation(request.Context(), input.Kind, input.DryerProfile)
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
