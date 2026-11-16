package httpapi

import (
	"fmt"
	"net/http"
	"strconv"

	"orderflow/internal/domain"
)

func (a *API) placeOrder(w http.ResponseWriter, r *http.Request) {
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 || !domain.ValidKey(keys[0]) {
		a.handleError(w, fmt.Errorf("%w: supply one valid Idempotency-Key", domain.ErrInvalid))
		return
	}
	var input domain.OrderInput
	if !decode(w, r, &input) {
		return
	}
	if err := input.Normalize(); err != nil {
		a.handleError(w, err)
		return
	}
	order, replayed, err := a.db.PlaceOrder(r.Context(), keys[0], input)
	if err != nil {
		a.handleError(w, err)
		return
	}
	w.Header().Set("Location", fmt.Sprintf("/v1/orders/%d", order.ID))
	if replayed {
		w.Header().Set("Idempotency-Replayed", "true")
		writeJSON(w, http.StatusOK, order)
		return
	}
	writeJSON(w, http.StatusAccepted, order)
}

func (a *API) listOrders(w http.ResponseWriter, r *http.Request) {
	var cursor int64
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || id < 1 {
			a.handleError(w, fmt.Errorf("%w: invalid order cursor", domain.ErrInvalid))
			return
		}
		cursor = id
	}
	limit, err := pageLimit(r)
	if err != nil {
		a.handleError(w, err)
		return
	}
	orders, err := a.db.Orders(r.Context(), cursor, limit+1)
	if err != nil {
		a.handleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, paginate(orders, limit, func(o domain.Order) int64 { return o.ID }))
}

func (a *API) getOrder(w http.ResponseWriter, r *http.Request) {
	id, ok := a.orderID(w, r)
	if !ok {
		return
	}
	order, err := a.db.Order(r.Context(), id)
	if err != nil {
		a.handleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, order)
}

func (a *API) cancelOrder(w http.ResponseWriter, r *http.Request) {
	id, ok := a.orderID(w, r)
	if !ok {
		return
	}
	order, err := a.db.CancelOrder(r.Context(), id)
	if err != nil {
		a.handleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, order)
}

func (a *API) timeline(w http.ResponseWriter, r *http.Request) {
	id, ok := a.orderID(w, r)
	if !ok {
		return
	}
	events, err := a.db.Timeline(r.Context(), id)
	if err != nil {
		a.handleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (a *API) orderID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		a.handleError(w, fmt.Errorf("%w: invalid order ID", domain.ErrInvalid))
		return 0, false
	}
	return id, true
}
