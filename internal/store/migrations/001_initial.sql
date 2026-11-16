CREATE TABLE products (
    sku text PRIMARY KEY CHECK (sku ~ '^[A-Z0-9][A-Z0-9_-]{0,31}$'),
    name text NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 120),
    price_cents bigint NOT NULL CHECK (price_cents BETWEEN 1 AND 100000000),
    on_hand integer NOT NULL CHECK (on_hand BETWEEN 0 AND 1000000000),
    reserved integer NOT NULL DEFAULT 0 CHECK (reserved BETWEEN 0 AND on_hand)
);

CREATE TABLE orders (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    customer text NOT NULL CHECK (octet_length(customer) BETWEEN 1 AND 120),
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'confirmed', 'rejected', 'cancelled')),
    reason text NOT NULL DEFAULT '',
    total_cents bigint NOT NULL DEFAULT 0 CHECK (total_cents >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_id bigint NOT NULL REFERENCES orders(id),
    sku text NOT NULL REFERENCES products(sku),
    quantity integer NOT NULL CHECK (quantity BETWEEN 1 AND 1000),
    unit_price_cents bigint NOT NULL CHECK (unit_price_cents BETWEEN 1 AND 100000000),
    PRIMARY KEY (order_id, sku)
);

-- A single row serves both the order timeline and the transactional outbox.
CREATE TABLE order_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id bigint NOT NULL REFERENCES orders(id),
    event_type text NOT NULL,
    status text NOT NULL,
    reason text NOT NULL DEFAULT '',
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    UNIQUE (order_id, event_type),
    CHECK (
        (event_type = 'order.placed' AND status = 'pending') OR
        (event_type = 'order.confirmed' AND status = 'confirmed') OR
        (event_type = 'order.rejected' AND status = 'rejected') OR
        (event_type = 'order.cancelled' AND status = 'cancelled')
    )
);

CREATE INDEX order_events_timeline_idx ON order_events (order_id, id);
CREATE INDEX order_events_outbox_idx ON order_events (id) WHERE published_at IS NULL;
