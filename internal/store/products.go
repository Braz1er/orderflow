package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"orderflow/internal/domain"
)

const productColumns = "sku, name, price_cents, on_hand - reserved AS available"

func (s *Store) CreateProduct(ctx context.Context, p domain.Product) error {
	_, err := s.pool.Exec(ctx,
		"INSERT INTO products (sku, name, price_cents, on_hand) VALUES ($1, $2, $3, $4)",
		p.SKU, p.Name, p.PriceCents, p.Available)
	if uniqueViolation(err) {
		return fmt.Errorf("%w: SKU already exists", domain.ErrConflict)
	}
	return err
}

func (s *Store) Product(ctx context.Context, sku string) (domain.Product, error) {
	var p domain.Product
	err := s.pool.QueryRow(ctx, "SELECT "+productColumns+" FROM products WHERE sku = $1", sku).
		Scan(&p.SKU, &p.Name, &p.PriceCents, &p.Available)
	return p, notFound(err)
}

func (s *Store) Products(ctx context.Context, after string, limit int) ([]domain.Product, error) {
	rows, err := s.pool.Query(ctx, "SELECT "+productColumns+" FROM products WHERE sku > $1 ORDER BY sku LIMIT $2", after, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByName[domain.Product])
}

func (s *Store) Restock(ctx context.Context, sku string, quantity int) (domain.Product, error) {
	var p domain.Product
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var onHand int
		if err := tx.QueryRow(ctx, "SELECT on_hand FROM products WHERE sku = $1 FOR UPDATE", sku).Scan(&onHand); err != nil {
			return notFound(err)
		}
		if quantity > domain.MaxUnits-onHand {
			return fmt.Errorf("%w: stock capacity exceeded", domain.ErrConflict)
		}
		return tx.QueryRow(ctx, "UPDATE products SET on_hand = on_hand + $2 WHERE sku = $1 RETURNING "+productColumns,
			sku, quantity).Scan(&p.SKU, &p.Name, &p.PriceCents, &p.Available)
	})
	return p, err
}
