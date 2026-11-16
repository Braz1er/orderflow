package domain

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

var (
	ErrInvalid  = errors.New("invalid input")
	ErrNotFound = errors.New("resource not found")
	ErrConflict = errors.New("resource conflict")
	skuPattern  = regexp.MustCompile(`^[A-Z0-9][A-Z0-9_-]{0,31}$`)
	keyPattern  = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
)

const (
	Pending   = "pending"
	Confirmed = "confirmed"
	Rejected  = "rejected"
	Cancelled = "cancelled"

	OrderPlaced    = "order.placed"
	OrderConfirmed = "order.confirmed"
	OrderRejected  = "order.rejected"
	OrderCancelled = "order.cancelled"

	MaxUnits = 1_000_000_000
)

type Product struct {
	SKU        string `json:"sku"`
	Name       string `json:"name"`
	PriceCents int64  `json:"price_cents"`
	Available  int    `json:"available"`
}

type LineInput struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type OrderInput struct {
	Customer string      `json:"customer"`
	Items    []LineInput `json:"items"`
}

type Line struct {
	LineInput
	UnitPriceCents int64 `json:"unit_price_cents"`
}

type Order struct {
	ID         int64     `json:"id"`
	Customer   string    `json:"customer"`
	Status     string    `json:"status"`
	Reason     string    `json:"reason,omitempty"`
	TotalCents int64     `json:"total_cents"`
	Items      []Line    `json:"items,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type Event struct {
	ID         int64     `json:"id"`
	OrderID    int64     `json:"order_id"`
	Type       string    `json:"type"`
	Status     string    `json:"status"`
	Reason     string    `json:"reason,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`
}

func ValidSKU(sku string) bool { return skuPattern.MatchString(sku) }
func ValidKey(key string) bool { return keyPattern.MatchString(key) }

func (p *Product) Validate() error {
	p.Name = strings.TrimSpace(p.Name)
	if !ValidSKU(p.SKU) || p.Name == "" || strings.ContainsRune(p.Name, 0) || len(p.Name) > 120 {
		return fmt.Errorf("%w: provide a valid SKU and a name of 1–120 bytes", ErrInvalid)
	}
	if p.PriceCents < 1 || p.PriceCents > 100_000_000 || p.Available < 0 || p.Available > MaxUnits {
		return fmt.Errorf("%w: price_cents must be 1–100000000 and available must be 0–%d", ErrInvalid, MaxUnits)
	}
	return nil
}

// Normalize validates the order and sorts its lines by SKU. This gives both
// request hashes and inventory locks a stable ordering.
func (o *OrderInput) Normalize() error {
	o.Customer = strings.TrimSpace(o.Customer)
	if o.Customer == "" || strings.ContainsRune(o.Customer, 0) || len(o.Customer) > 120 || len(o.Items) < 1 || len(o.Items) > 25 {
		return fmt.Errorf("%w: provide a customer of 1–120 bytes and 1–25 items", ErrInvalid)
	}
	seen := make(map[string]bool, len(o.Items))
	for _, item := range o.Items {
		if !ValidSKU(item.SKU) || item.Quantity < 1 || item.Quantity > 1000 || seen[item.SKU] {
			return fmt.Errorf("%w: each SKU must be valid and unique, with a quantity of 1–1000", ErrInvalid)
		}
		seen[item.SKU] = true
	}
	sort.Slice(o.Items, func(i, j int) bool { return o.Items[i].SKU < o.Items[j].SKU })
	return nil
}
