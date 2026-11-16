package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"orderflow/internal/domain"
)

const orderColumns = "id, customer, status, reason, total_cents, created_at, updated_at"

func scanOrder(row pgx.CollectableRow) (domain.Order, error) {
	var o domain.Order
	err := row.Scan(&o.ID, &o.Customer, &o.Status, &o.Reason, &o.TotalCents, &o.CreatedAt, &o.UpdatedAt)
	return o, notFound(err)
}

func (s *Store) PlaceOrder(ctx context.Context, key string, input domain.OrderInput) (domain.Order, bool, error) {
	payload, err := json.Marshal(input)
	if err != nil {
		return domain.Order{}, false, err
	}
	hash := sha256.Sum256(payload)
	var order domain.Order
	var replayed bool
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var id int64
		err := tx.QueryRow(ctx, `
			INSERT INTO orders (idempotency_key, request_hash, customer) VALUES ($1, $2, $3)
			ON CONFLICT (idempotency_key) DO NOTHING RETURNING id`, key, hash[:], input.Customer).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			var previousHash []byte
			if err := tx.QueryRow(ctx, "SELECT id, request_hash FROM orders WHERE idempotency_key = $1", key).
				Scan(&id, &previousHash); err != nil {
				return err
			}
			if !bytes.Equal(hash[:], previousHash) {
				return fmt.Errorf("%w: idempotency key already used for a different order", domain.ErrConflict)
			}
			replayed = true
			order, err = getOrder(ctx, tx, id)
			return err
		}
		if err != nil {
			return err
		}

		var total int64
		for _, item := range input.Items {
			var price int64
			if err := tx.QueryRow(ctx, "SELECT price_cents FROM products WHERE sku = $1", item.SKU).Scan(&price); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return fmt.Errorf("%w: unknown SKU %s", domain.ErrInvalid, item.SKU)
				}
				return err
			}
			if _, err := tx.Exec(ctx, "INSERT INTO order_items VALUES ($1, $2, $3, $4)", id, item.SKU, item.Quantity, price); err != nil {
				return err
			}
			total += price * int64(item.Quantity)
		}
		if _, err := tx.Exec(ctx, "UPDATE orders SET total_cents = $2 WHERE id = $1", id, total); err != nil {
			return err
		}
		if err := appendEvent(ctx, tx, id, domain.OrderPlaced, domain.Pending, ""); err != nil {
			return err
		}
		order, err = getOrder(ctx, tx, id)
		return err
	})
	return order, replayed, err
}

type orderReader interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func getOrder(ctx context.Context, db orderReader, id int64) (domain.Order, error) {
	rows, err := db.Query(ctx, "SELECT "+orderColumns+" FROM orders WHERE id = $1", id)
	if err != nil {
		return domain.Order{}, err
	}
	order, err := pgx.CollectOneRow(rows, scanOrder)
	if err != nil {
		return domain.Order{}, notFound(err)
	}
	rows, err = db.Query(ctx, `
		SELECT sku, quantity, unit_price_cents FROM order_items WHERE order_id = $1 ORDER BY sku`, id)
	if err != nil {
		return domain.Order{}, err
	}
	order.Items, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Line, error) {
		var item domain.Line
		err := row.Scan(&item.SKU, &item.Quantity, &item.UnitPriceCents)
		return item, err
	})
	return order, err
}

func (s *Store) Order(ctx context.Context, id int64) (domain.Order, error) {
	return getOrder(ctx, s.pool, id)
}

func (s *Store) Orders(ctx context.Context, before int64, limit int) ([]domain.Order, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+orderColumns+` FROM orders
		WHERE ($1 = 0 OR id < $1) ORDER BY id DESC LIMIT $2`, before, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanOrder)
}

func (s *Store) CancelOrder(ctx context.Context, id int64) (domain.Order, error) {
	var order domain.Order
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var status string
		if err := tx.QueryRow(ctx, "SELECT status FROM orders WHERE id = $1 FOR UPDATE", id).Scan(&status); err != nil {
			return notFound(err)
		}
		switch status {
		case domain.Confirmed:
			if _, err := hasStock(ctx, tx, id); err != nil {
				return err
			}
			if err := adjustReservation(ctx, tx, id, -1); err != nil {
				return err
			}
		case domain.Pending, domain.Cancelled:
		default:
			return fmt.Errorf("%w: only pending or confirmed orders can be cancelled", domain.ErrConflict)
		}
		if status != domain.Cancelled {
			if err := transition(ctx, tx, id, domain.OrderCancelled, domain.Cancelled, ""); err != nil {
				return err
			}
		}
		var err error
		order, err = getOrder(ctx, tx, id)
		return err
	})
	return order, err
}

func (s *Store) ReserveOrder(ctx context.Context, event domain.Event) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var status string
		err := tx.QueryRow(ctx, `
			SELECT o.status FROM orders o
			JOIN order_events e ON e.order_id = o.id
			WHERE o.id = $1 AND e.id = $2 AND e.event_type = $3 FOR UPDATE OF o`,
			event.OrderID, event.ID, domain.OrderPlaced).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil || status != domain.Pending {
			return err
		}
		available, err := hasStock(ctx, tx, event.OrderID)
		if err != nil {
			return err
		}
		if !available {
			return transition(ctx, tx, event.OrderID, domain.OrderRejected, domain.Rejected, "insufficient_stock")
		}
		if err := adjustReservation(ctx, tx, event.OrderID, 1); err != nil {
			return err
		}
		return transition(ctx, tx, event.OrderID, domain.OrderConfirmed, domain.Confirmed, "")
	})
}

// FOR NO KEY UPDATE locks all products in SKU order before either reservation or release.
func hasStock(ctx context.Context, tx pgx.Tx, id int64) (bool, error) {
	rows, err := tx.Query(ctx, `
		SELECT p.on_hand - p.reserved >= i.quantity
		FROM order_items i JOIN products p ON p.sku = i.sku
		WHERE i.order_id = $1 ORDER BY p.sku FOR NO KEY UPDATE OF p`, id)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	enough := true
	for rows.Next() {
		var available bool
		if err := rows.Scan(&available); err != nil {
			return false, err
		}
		enough = enough && available
	}
	return enough, rows.Err()
}

func adjustReservation(ctx context.Context, tx pgx.Tx, id int64, direction int) error {
	_, err := tx.Exec(ctx, `
		UPDATE products p SET reserved = p.reserved + i.quantity * $2
		FROM order_items i WHERE i.order_id = $1 AND p.sku = i.sku`, id, direction)
	return err
}

func transition(ctx context.Context, tx pgx.Tx, id int64, eventType, status, reason string) error {
	if _, err := tx.Exec(ctx, "UPDATE orders SET status = $2, reason = $3, updated_at = clock_timestamp() WHERE id = $1",
		id, status, reason); err != nil {
		return err
	}
	return appendEvent(ctx, tx, id, eventType, status, reason)
}
