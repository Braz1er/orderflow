# OrderFlow

Сервис управления заказами и складскими резервами на Go. Клиент отправляет корзину в REST API и получает номер заказа. Фоновый воркер обрабатывает событие из Kafka и либо резервирует все позиции, либо отклоняет заказ с причиной `insufficient_stock`. Отмена подтверждённого заказа возвращает товар в доступный остаток.

## Быстрый старт

Нужны Docker и Docker Compose. PostgreSQL, Kafka и Redis запускаются внутри Compose, миграции и создание топика выполняются автоматически.

```bash
cp .env.example .env
docker compose up --build --wait
curl -fsS http://127.0.0.1:8080/readyz
```

Скрипт демонстрации использует `curl` и `jq`. Он создаёт товар, оформляет заказ, ждёт подтверждения, проверяет остаток, отменяет заказ и выводит историю событий.

```bash
./scripts/demo.sh
```

Для другого порта или токена передайте скрипту `ORDERFLOW_URL` и `API_TOKEN`.

```bash
docker compose logs -f api worker
docker compose down
```

При обычной остановке PostgreSQL и Kafka сохраняют данные в именованных томах.

## API

Все маршруты `/v1` принимают заголовок `Authorization: Bearer <API_TOKEN>`. Запросы с телом используют `Content-Type: application/json`. Контракт доступен в [openapi.yaml](openapi.yaml).

| Метод | Маршрут | Действие |
| --- | --- | --- |
| GET | `/healthz` | Проверить HTTP-процесс |
| GET | `/readyz` | Проверить доступность PostgreSQL и Redis |
| POST | `/v1/products` | Создать товар с ценой в центах и начальным остатком |
| GET | `/v1/products` | Получить каталог с пагинацией по SKU |
| GET | `/v1/products/{sku}` | Получить товар и доступный остаток |
| POST | `/v1/products/{sku}/restock` | Увеличить складской остаток |
| POST | `/v1/orders` | Принять заказ с заголовком `Idempotency-Key` |
| GET | `/v1/orders` | Получить список заказов с пагинацией по ID |
| GET | `/v1/orders/{id}` | Получить статус, позиции и сумму заказа |
| POST | `/v1/orders/{id}/cancel` | Отменить заказ |
| GET | `/v1/orders/{id}/events` | Получить историю состояний заказа |

Пример запроса после создания товара с SKU `KEYBOARD-01`.

```bash
curl -i http://127.0.0.1:8080/v1/orders \
  -H 'Authorization: Bearer orderflow-demo-token' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: checkout-alex-001' \
  -d '{"customer":"Алекс","items":[{"sku":"KEYBOARD-01","quantity":2}]}'
```

Первый запрос возвращает `202 Accepted` и `Location`. Повтор с тем же ключом и той же корзиной возвращает текущий заказ с `200 OK`, даже если позиции переставлены. Тот же ключ с изменёнными данными возвращает `409 Conflict`. Для списков передайте `next_cursor` из ответа как `cursor` следующего запроса.

## Архитектура

```mermaid
flowchart LR
    Client["Клиент"] --> API["Go · REST API"]
    API --> Redis["Redis · лимит запросов"]
    API --> DB["PostgreSQL · заказы, склад, outbox"]
    DB --> Relay["Воркер · Outbox relay"]
    Relay --> Kafka["Kafka · orderflow.orders.v1"]
    Kafka --> Consumer["Воркер · резервирование"]
    Consumer --> DB
```

1. API фиксирует цену каждой позиции и записывает заказ вместе с событием `order.placed` в одной транзакции PostgreSQL.
2. Relay читает неопубликованные события из `order_events` и отправляет их в Kafka с ключом `order_id`. Строка помечается опубликованной после подтверждения брокера. Один relay публикует события в каждый момент времени благодаря advisory lock.
3. Consumer блокирует строку заказа и товары в порядке SKU. Он проверяет остатки всех позиций и атомарно меняет складской резерв, статус и историю. Kafka offset фиксируется после коммита PostgreSQL.
4. Публикация имеет семантику at-least-once. Если событие доставлено повторно, проверка статуса под блокировкой предотвращает повторный резерв. Redis считает запросы на IP в 60-секундном окне, а при превышении лимита API возвращает `429` и `Retry-After`.

```mermaid
stateDiagram-v2
    [*] --> pending
    pending --> confirmed: резерв создан
    pending --> rejected: недостаточно товара
    pending --> cancelled: отмена до резерва
    confirmed --> cancelled: резерв освобождён
```

Топик содержит четыре типа событий. У каждого сообщения есть ID события, ID заказа, тип, статус и время. Событие отклонения содержит причину. История в API читается напрямую из PostgreSQL.

## Настройки

| Переменная | Значение в Compose |
| --- | --- |
| `API_TOKEN` | Берётся из `.env`, от 16 символов |
| `API_PORT` | `8080`, HTTP доступен на `127.0.0.1` |
| `RATE_LIMIT_PER_MINUTE` | `120` |
| `DATABASE_URL` | PostgreSQL внутри сети Compose |
| `REDIS_ADDR` | `redis:6379` |
| `KAFKA_BROKERS` | `kafka:9092` |
| `KAFKA_TOPIC` | `orderflow.orders.v1` |
| `KAFKA_GROUP` | `orderflow-inventory-v1` |

Первые три переменные настраиваются через `.env`. Остальные передаются процессам через `environment` в Compose или через окружение при прямом запуске. В локальном стенде один брокер Kafka, общий токен оператора и HTTP на loopback. Управление заказом охватывает резервирование и отмену.

## Разработка и проверки

Код API находится в `internal/httpapi`, SQL и миграции — в `internal/store`, публикация и обработка событий — в `internal/messaging`. Один бинарник имеет режимы `api`, `worker`, `migrate` и `healthcheck`.

Для локальной сборки нужен Go 1.26 или новее. Интеграционные тесты используют уже запущенный стенд и создают свои товары и заказы.

```bash
go build ./...
go vet ./...
ORDERFLOW_URL=http://127.0.0.1:8080 API_TOKEN=orderflow-demo-token \
  go test -count=1 -tags=integration -v ./integration
```
