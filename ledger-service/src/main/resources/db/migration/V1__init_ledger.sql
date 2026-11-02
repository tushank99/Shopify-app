-- One row per consumed order.placed event.
-- event_id (the Node outbox eventId) is the PRIMARY KEY, so "INSERT ... ON CONFLICT (event_id) DO NOTHING"
-- is the idempotency guard: a redelivered or replayed event can never create a second row.
CREATE TABLE ledger_entry (
    event_id          UUID          PRIMARY KEY,
    order_id          VARCHAR(24)   NOT NULL,
    user_id           VARCHAR(24)   NOT NULL,
    event_type        VARCHAR(64)   NOT NULL,
    -- The contract allows up to 12 whole digits + 2 decimals => NUMERIC(14,2).
    total_price       NUMERIC(14,2) NOT NULL,
    currency          VARCHAR(3)    NOT NULL,
    order_created_at  TIMESTAMPTZ   NOT NULL,
    ingested_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),

    -- Second guard: the same business order can never be ledgered twice, even under a different eventId.
    CONSTRAINT uq_ledger_entry_order_event UNIQUE (order_id, event_type),

    CONSTRAINT ck_ledger_entry_order_id_len CHECK (char_length(order_id) = 24),
    CONSTRAINT ck_ledger_entry_user_id_len  CHECK (char_length(user_id) = 24),
    CONSTRAINT ck_ledger_entry_currency_len CHECK (char_length(currency) = 3),
    CONSTRAINT ck_ledger_entry_total_nonneg CHECK (total_price >= 0)
);

-- GET /api/ledger/users/{userId}: newest first, stable tie-breaker for pagination.
CREATE INDEX idx_ledger_entry_user_created
    ON ledger_entry (user_id, order_created_at DESC, event_id DESC);

CREATE TABLE ledger_item (
    event_id    UUID          NOT NULL REFERENCES ledger_entry (event_id) ON DELETE CASCADE,
    line_no     INTEGER       NOT NULL,
    product_id  VARCHAR(24)   NOT NULL,
    quantity    INTEGER       NOT NULL,
    unit_price  NUMERIC(12,2) NOT NULL,
    line_total  NUMERIC(14,2) NOT NULL,

    PRIMARY KEY (event_id, line_no),

    CONSTRAINT ck_ledger_item_product_id_len CHECK (char_length(product_id) = 24),
    CONSTRAINT ck_ledger_item_quantity_pos   CHECK (quantity > 0),
    CONSTRAINT ck_ledger_item_unit_nonneg    CHECK (unit_price >= 0),
    CONSTRAINT ck_ledger_item_line_nonneg    CHECK (line_total >= 0)
);

CREATE INDEX idx_ledger_item_product ON ledger_item (product_id);