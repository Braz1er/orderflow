package httpapi

import (
	"fmt"
	"net/http"

	"orderflow/internal/domain"
)

func (a *API) createProduct(w http.ResponseWriter, r *http.Request) {
	var product domain.Product
	if !decode(w, r, &product) {
		return
	}
	if err := product.Validate(); err != nil {
		a.handleError(w, err)
		return
	}
	if err := a.db.CreateProduct(r.Context(), product); err != nil {
		a.handleError(w, err)
		return
	}
	w.Header().Set("Location", "/v1/products/"+product.SKU)
	writeJSON(w, http.StatusCreated, product)
}

func (a *API) listProducts(w http.ResponseWriter, r *http.Request) {
	after := r.URL.Query().Get("cursor")
	if after != "" && !domain.ValidSKU(after) {
		a.handleError(w, fmt.Errorf("%w: invalid product cursor", domain.ErrInvalid))
		return
	}
	limit, err := pageLimit(r)
	if err != nil {
		a.handleError(w, err)
		return
	}
	products, err := a.db.Products(r.Context(), after, limit+1)
	if err != nil {
		a.handleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, paginate(products, limit, func(p domain.Product) string { return p.SKU }))
}

func (a *API) getProduct(w http.ResponseWriter, r *http.Request) {
	sku := r.PathValue("sku")
	if !domain.ValidSKU(sku) {
		a.handleError(w, fmt.Errorf("%w: invalid SKU", domain.ErrInvalid))
		return
	}
	product, err := a.db.Product(r.Context(), sku)
	if err != nil {
		a.handleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, product)
}

func (a *API) restock(w http.ResponseWriter, r *http.Request) {
	sku := r.PathValue("sku")
	if !domain.ValidSKU(sku) {
		a.handleError(w, fmt.Errorf("%w: invalid SKU", domain.ErrInvalid))
		return
	}
	var input struct {
		Quantity int `json:"quantity"`
	}
	if !decode(w, r, &input) {
		return
	}
	if input.Quantity < 1 || input.Quantity > domain.MaxUnits {
		a.handleError(w, fmt.Errorf("%w: quantity must be 1–%d", domain.ErrInvalid, domain.MaxUnits))
		return
	}
	product, err := a.db.Restock(r.Context(), sku, input.Quantity)
	if err != nil {
		a.handleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, product)
}
