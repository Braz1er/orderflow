//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"orderflow/internal/domain"
)

type client struct {
	baseURL string
	token   string
	http    *http.Client
}

type response struct {
	code   int
	header http.Header
	body   []byte
	err    error
}

func (c client) request(ctx context.Context, method, path, key string, body any) response {
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		if err != nil {
			return response{err: err}
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return response{err: err}
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return response{err: err}
	}
	defer resp.Body.Close()
	result, err := io.ReadAll(resp.Body)
	return response{code: resp.StatusCode, header: resp.Header, body: result, err: err}
}

func (r response) expect(t *testing.T, status int) {
	t.Helper()
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r.code != status {
		t.Fatalf("status = %d, want %d, body = %s", r.code, status, r.body)
	}
}

func bodyAs[T any](t *testing.T, r response, status int) T {
	t.Helper()
	r.expect(t, status)
	var result T
	if err := json.Unmarshal(r.body, &result); err != nil {
		t.Fatalf("decode %s: %v", r.body, err)
	}
	return result
}

func getClient(t *testing.T) client {
	t.Helper()
	baseURL, token := os.Getenv("ORDERFLOW_URL"), os.Getenv("API_TOKEN")
	if baseURL == "" || token == "" {
		t.Fatal("set ORDERFLOW_URL and API_TOKEN to run integration tests")
	}
	return client{baseURL: baseURL, token: token, http: &http.Client{Timeout: 5 * time.Second}}
}

func (c client) createProduct(t *testing.T, sku string, available int) {
	t.Helper()
	c.request(t.Context(), http.MethodPost, "/v1/products", "", domain.Product{
		SKU: sku, Name: "Integration fixture", PriceCents: 1250, Available: available,
	}).expect(t, http.StatusCreated)
}

func (c client) product(t *testing.T, sku string) domain.Product {
	t.Helper()
	return bodyAs[domain.Product](t, c.request(t.Context(), http.MethodGet, "/v1/products/"+sku, "", nil), http.StatusOK)
}

func (c client) place(ctx context.Context, key string, items ...domain.LineInput) response {
	return c.request(ctx, http.MethodPost, "/v1/orders", key, domain.OrderInput{Customer: "Integration customer", Items: items})
}

func orderPath(id int64) string { return fmt.Sprintf("/v1/orders/%d", id) }

func (c client) settled(t *testing.T, id int64) domain.Order {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		order := bodyAs[domain.Order](t, c.request(t.Context(), http.MethodGet, orderPath(id), "", nil), http.StatusOK)
		if order.Status != domain.Pending {
			return order
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("order %d stayed pending", id)
	return domain.Order{}
}

func TestReservationAndCancellation(t *testing.T) {
	c := getClient(t)
	c.request(t.Context(), http.MethodGet, "/readyz", "", nil).expect(t, http.StatusOK)
	anonymous := c
	anonymous.token = ""
	anonymous.request(t.Context(), http.MethodGet, "/v1/orders", "", nil).expect(t, http.StatusUnauthorized)

	skuA, skuB := "A-"+rand.Text(), "B-"+rand.Text()
	c.createProduct(t, skuA, 2)
	c.createProduct(t, skuB, 2)

	// The list query and cursor are covered against a real database.
	type productPage struct {
		Items []domain.Product `json:"items"`
		Next  string           `json:"next_cursor"`
	}
	first := bodyAs[productPage](t, c.request(t.Context(), http.MethodGet, "/v1/products?limit=1", "", nil), http.StatusOK)
	if len(first.Items) != 1 || first.Next != first.Items[0].SKU {
		t.Fatalf("invalid product page: %+v", first)
	}
	second := bodyAs[productPage](t, c.request(t.Context(), http.MethodGet, "/v1/products?limit=1&cursor="+first.Next, "", nil), http.StatusOK)
	if len(second.Items) != 1 || second.Items[0].SKU == first.Items[0].SKU {
		t.Fatalf("invalid next product page: %+v", second)
	}

	a := domain.LineInput{SKU: skuA, Quantity: 2}
	b := domain.LineInput{SKU: skuB, Quantity: 1}
	key := rand.Text()
	order := bodyAs[domain.Order](t, c.place(t.Context(), key, b, a), http.StatusAccepted)
	if order.Status != domain.Pending || order.TotalCents != 3750 {
		t.Fatalf("invalid new order: %+v", order)
	}
	replay := c.place(t.Context(), key, a, b)
	existing := bodyAs[domain.Order](t, replay, http.StatusOK)
	if existing.ID != order.ID || replay.header.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("invalid replay: %+v", existing)
	}
	c.place(t.Context(), key, a).expect(t, http.StatusConflict)

	if settled := c.settled(t, order.ID); settled.Status != domain.Confirmed {
		t.Fatalf("order did not confirm: %+v", settled)
	}
	if p := c.product(t, skuA); p.Available != 0 {
		t.Fatalf("expected SKU %s to be reserved, got %+v", skuA, p)
	}
	for range 2 {
		cancelled := bodyAs[domain.Order](t, c.request(t.Context(), http.MethodPost, orderPath(order.ID)+"/cancel", "", nil), http.StatusOK)
		if cancelled.Status != domain.Cancelled {
			t.Fatalf("order did not cancel: %+v", cancelled)
		}
	}
	if p := c.product(t, skuA); p.Available != 2 {
		t.Fatalf("expected released stock, got %+v", p)
	}
	if p := c.product(t, skuB); p.Available != 2 {
		t.Fatalf("expected released stock, got %+v", p)
	}

	events := bodyAs[[]domain.Event](t, c.request(t.Context(), http.MethodGet, orderPath(order.ID)+"/events", "", nil), http.StatusOK)
	want := []string{domain.OrderPlaced, domain.OrderConfirmed, domain.OrderCancelled}
	if len(events) != len(want) {
		t.Fatalf("invalid timeline: %+v", events)
	}
	for i, event := range events {
		if event.Type != want[i] || event.OrderID != order.ID {
			t.Fatalf("invalid timeline: %+v", events)
		}
	}

	updated := bodyAs[domain.Product](t, c.request(t.Context(), http.MethodPost, "/v1/products/"+skuA+"/restock", "", map[string]int{"quantity": 3}), http.StatusOK)
	if updated.Available != 5 {
		t.Fatalf("expected replenished stock, got %+v", updated)
	}
}

func TestCompetingOrdersDoNotOversell(t *testing.T) {
	c := getClient(t)
	shared, extra := "A-"+rand.Text(), "B-"+rand.Text()
	c.createProduct(t, shared, 1)
	c.createProduct(t, extra, 2)
	items := []domain.LineInput{{SKU: shared, Quantity: 1}, {SKU: extra, Quantity: 1}}

	var requests sync.WaitGroup
	results := make([]response, 2)
	for i := range results {
		requests.Go(func() {
			results[i] = c.place(t.Context(), rand.Text(), items...)
		})
	}
	requests.Wait()

	statuses := make(map[string]int)
	for _, result := range results {
		order := bodyAs[domain.Order](t, result, http.StatusAccepted)
		settled := c.settled(t, order.ID)
		statuses[settled.Status]++
		if settled.Status == domain.Rejected && settled.Reason != "insufficient_stock" {
			t.Fatalf("invalid rejection: %+v", settled)
		}
	}
	if statuses[domain.Confirmed] != 1 || statuses[domain.Rejected] != 1 {
		t.Fatalf("expected one confirmation and one rejection, got %v", statuses)
	}
	if p := c.product(t, shared); p.Available != 0 {
		t.Fatalf("expected last item to be reserved, got %+v", p)
	}
	if p := c.product(t, extra); p.Available != 1 {
		t.Fatalf("expected no partial reservation for the rejected order, got %+v", p)
	}
}

func TestConcurrentIdempotency(t *testing.T) {
	c := getClient(t)
	sku, key := "IDEM-"+rand.Text(), rand.Text()
	c.createProduct(t, sku, 4)

	var requests sync.WaitGroup
	results := make([]response, 4)
	for i := range results {
		requests.Go(func() {
			results[i] = c.place(t.Context(), key, domain.LineInput{SKU: sku, Quantity: 1})
		})
	}
	requests.Wait()

	var id int64
	created := 0
	for _, result := range results {
		status := http.StatusOK
		if result.code == http.StatusAccepted {
			status = http.StatusAccepted
			created++
		}
		order := bodyAs[domain.Order](t, result, status)
		if id == 0 {
			id = order.ID
		}
		if order.ID != id {
			t.Fatalf("idempotent requests created multiple orders: %d and %d", id, order.ID)
		}
	}
	if created != 1 {
		t.Fatalf("expected exactly one 202, got %d", created)
	}
	if order := c.settled(t, id); order.Status != domain.Confirmed {
		t.Fatalf("expected confirmation, got %+v", order)
	}
	if p := c.product(t, sku); p.Available != 3 {
		t.Fatalf("expected exactly one reservation, got %+v", p)
	}

	type orderPage struct {
		Items []domain.Order `json:"items"`
		Next  int64          `json:"next_cursor"`
	}
	page := bodyAs[orderPage](t, c.request(t.Context(), http.MethodGet, "/v1/orders?limit=1", "", nil), http.StatusOK)
	if len(page.Items) != 1 || page.Items[0].ID != id || len(page.Items[0].Items) != 0 {
		t.Fatalf("invalid order summary: %+v", page)
	}
	if page.Next != 0 {
		path := fmt.Sprintf("/v1/orders?limit=1&cursor=%d", page.Next)
		next := bodyAs[orderPage](t, c.request(t.Context(), http.MethodGet, path, "", nil), http.StatusOK)
		if len(next.Items) != 1 || next.Items[0].ID >= id {
			t.Fatalf("invalid order cursor: %+v", next)
		}
	}
}
