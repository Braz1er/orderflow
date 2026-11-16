#!/usr/bin/env bash
set -euo pipefail

base_url="${ORDERFLOW_URL:-http://127.0.0.1:8080}"
token="${API_TOKEN:-orderflow-demo-token}"
sku="DEMO-$(date +%s)-${RANDOM}"

api() {
  curl --fail-with-body --silent --show-error --max-time 10 \
    -H "Authorization: Bearer $token" -H 'Content-Type: application/json' "$@"
}

printf 'Создаём товар с двумя единицами на складе.\n'
api "$base_url/v1/products" -d "{\"sku\":\"$sku\",\"name\":\"Механическая клавиатура\",\"price_cents\":9900,\"available\":2}" | jq

printf '\nОформляем заказ на обе единицы.\n'
order=$(api "$base_url/v1/orders" -H "Idempotency-Key: $sku" \
  -d "{\"customer\":\"Алекс\",\"items\":[{\"sku\":\"$sku\",\"quantity\":2}]}")
jq . <<< "$order"
id=$(jq -er .id <<< "$order")
order_url="$base_url/v1/orders/$id"

for _ in {1..30}; do
  order=$(api "$order_url")
  status=$(jq -er .status <<< "$order")
  if [[ "$status" != pending ]]; then
    break
  fi
  sleep 0.5
done
if [[ "$status" != confirmed ]]; then
  printf 'Ожидали confirmed, получили %s.\n' "$status" >&2
  exit 1
fi

printf '\nKafka передала заказ воркеру. Проверяем остаток.\n'
api "$base_url/v1/products/$sku" | jq -e 'select(.available == 0)'

printf '\nОтменяем заказ и проверяем освобождение резерва.\n'
api -X POST "$order_url/cancel" | jq -e 'select(.status == "cancelled")'
api "$base_url/v1/products/$sku" | jq -e 'select(.available == 2)'

printf '\nИстория заказа\n'
api "$order_url/events" | jq
