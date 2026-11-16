package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	DatabaseURL string
	RedisAddr   string
	Brokers     []string
	Topic       string
	Group       string
	HTTPAddr    string
	Token       string
	RateLimit   int
}

func Load(mode string) (Config, error) {
	cfg := Config{
		DatabaseURL: value("DATABASE_URL", "postgres://orderflow:orderflow@localhost:5432/orderflow?sslmode=disable"),
		RedisAddr:   value("REDIS_ADDR", "localhost:6379"),
		Brokers:     strings.FieldsFunc(value("KAFKA_BROKERS", "localhost:9092"), func(r rune) bool { return r == ',' || r == ' ' }),
		Topic:       value("KAFKA_TOPIC", "orderflow.orders.v1"),
		Group:       value("KAFKA_GROUP", "orderflow-inventory-v1"),
		HTTPAddr:    value("HTTP_ADDR", ":8080"),
		Token:       os.Getenv("API_TOKEN"),
	}
	switch mode {
	case "api":
		if len(cfg.Token) < 16 {
			return Config{}, fmt.Errorf("API_TOKEN must have at least 16 characters")
		}
		limit, err := strconv.Atoi(value("RATE_LIMIT_PER_MINUTE", "120"))
		if err != nil || limit < 1 {
			return Config{}, fmt.Errorf("RATE_LIMIT_PER_MINUTE must be a positive integer")
		}
		cfg.RateLimit = limit
	case "worker":
		if len(cfg.Brokers) == 0 {
			return Config{}, fmt.Errorf("KAFKA_BROKERS must contain at least one broker")
		}
	}
	return cfg, nil
}

func value(key, fallback string) string {
	if raw := os.Getenv(key); raw != "" {
		return raw
	}
	return fallback
}
