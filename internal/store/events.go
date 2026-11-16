package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"orderflow/internal/domain"
)

const eventColumns = "id, order_id, event_type, status, reason, occurred_at"

func scanEvent(row pgx.CollectableRow) (domain.Event, error) {
	var event domain.Event
	err := row.Scan(&event.ID, &event.OrderID, &event.Type, &event.Status, &event.Reason, &event.OccurredAt)
	return event, err
}

func appendEvent(ctx context.Context, tx pgx.Tx, id int64, eventType, status, reason string) error {
	_, err := tx.Exec(ctx, "INSERT INTO order_events (order_id, event_type, status, reason) VALUES ($1, $2, $3, $4)",
		id, eventType, status, reason)
	return err
}

func (s *Store) Timeline(ctx context.Context, id int64) ([]domain.Event, error) {
	rows, err := s.pool.Query(ctx, "SELECT "+eventColumns+" FROM order_events WHERE order_id = $1 ORDER BY id", id)
	if err != nil {
		return nil, err
	}
	events, err := pgx.CollectRows(rows, scanEvent)
	if err == nil && len(events) == 0 {
		return nil, domain.ErrNotFound
	}
	return events, err
}

// RelayOne serializes publishers across replicas. A broker ACK precedes marking
// the row as published, so a crash between those operations can redeliver it.
func (s *Store) RelayOne(ctx context.Context, publish func(context.Context, domain.Event) error) (bool, error) {
	var sent bool
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var locked bool
		if err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock(782601, 2)").Scan(&locked); err != nil || !locked {
			return err
		}
		rows, err := tx.Query(ctx, "SELECT "+eventColumns+" FROM order_events WHERE published_at IS NULL ORDER BY id LIMIT 1")
		if err != nil {
			return err
		}
		event, err := pgx.CollectOneRow(rows, scanEvent)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := publish(ctx, event); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, "UPDATE order_events SET published_at = clock_timestamp() WHERE id = $1", event.ID)
		sent = err == nil
		return err
	})
	return sent, err
}
