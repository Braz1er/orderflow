package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"orderflow/internal/config"
	"orderflow/internal/domain"
	"orderflow/internal/store"
)

func Run(ctx context.Context, db *store.Store, cfg config.Config, log *slog.Logger) error {
	producer, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		return err
	}
	defer producer.Close()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return err
	}
	defer consumer.CloseAllowingRebalance()

	ctx, cancel := context.WithCancel(ctx)
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		relay(ctx, db, producer, cfg.Topic, log)
	}()
	log.Info("worker started", "topic", cfg.Topic, "group", cfg.Group)
	err = consume(ctx, db, consumer, log)
	cancel()
	<-relayDone
	return err
}

func relay(ctx context.Context, db *store.Store, producer *kgo.Client, topic string, log *slog.Logger) {
	for ctx.Err() == nil {
		attempt, cancel := context.WithTimeout(ctx, 8*time.Second)
		sent, err := db.RelayOne(attempt, func(ctx context.Context, event domain.Event) error {
			payload, err := json.Marshal(event)
			if err != nil {
				return err
			}
			return producer.ProduceSync(ctx, &kgo.Record{
				Topic: topic,
				Key:   strconv.AppendInt(nil, event.OrderID, 10),
				Value: payload,
			}).FirstErr()
		})
		cancel()
		switch {
		case ctx.Err() != nil:
			return
		case err != nil:
			log.Warn("outbox publish failed", "error", err)
			pause(ctx, time.Second)
		case !sent:
			pause(ctx, 250*time.Millisecond)
		}
	}
}

func consume(ctx context.Context, db *store.Store, client *kgo.Client, log *slog.Logger) error {
	for ctx.Err() == nil {
		fetches := client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			log.Warn("kafka fetch failed", "topic", topic, "partition", partition, "error", err)
		})
		records := fetches.Records()
		if len(records) == 0 {
			pause(ctx, time.Second)
			continue
		}

		record := records[0]
		attempt, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := handleRecord(attempt, db, record, log)
		if err == nil {
			err = client.CommitRecords(attempt, record)
		}
		cancel()
		client.AllowRebalance()
		if err != nil && ctx.Err() == nil {
			// Restart from the last committed offset. Processing a later record
			// could otherwise commit past the failed one.
			return fmt.Errorf("process Kafka record at %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
		}
	}
	return nil
}

func handleRecord(ctx context.Context, db *store.Store, record *kgo.Record, log *slog.Logger) error {
	var event domain.Event
	err := json.Unmarshal(record.Value, &event)
	if err == nil && (event.ID < 1 || event.OrderID < 1 || event.Type == "") {
		err = errors.New("invalid event fields")
	}
	if err != nil {
		log.Warn("skipping malformed Kafka event", "partition", record.Partition, "offset", record.Offset, "error", err)
		return nil
	}
	if event.Type != domain.OrderPlaced {
		return nil
	}
	return db.ReserveOrder(ctx, event)
}

func pause(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}
