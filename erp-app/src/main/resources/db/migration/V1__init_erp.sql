-- ERP order-to-cash schema (MySQL 8, InnoDB). Flyway owns it; Hibernate only validates nothing (ddl-auto: none).

CREATE TABLE inventory_item (
    sku            VARCHAR(64)   NOT NULL,
    product_id     VARCHAR(24)   NOT NULL,
    name           VARCHAR(200)  NOT NULL,
    unit_price     DECIMAL(12,2) NOT NULL,
    unit_cost      DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    on_hand        INT           NOT NULL DEFAULT 0,
    reserved       INT           NOT NULL DEFAULT 0,
    reorder_point  INT           NOT NULL DEFAULT 0,
    reorder_qty    INT           NOT NULL DEFAULT 0,
    -- optimistic-locking counter (JPA @Version): UPDATE ... WHERE sku = ? AND version = ?
    version        BIGINT        NOT NULL DEFAULT 0,
    created_at     DATETIME(6)   NOT NULL,
    updated_at     DATETIME(6)   NOT NULL,
    PRIMARY KEY (sku),
    UNIQUE KEY uq_inventory_product_id (product_id),
    -- last line of defence against overselling, independent of application code
    CONSTRAINT ck_inventory_non_negative CHECK (on_hand >= 0 AND reserved >= 0),
    CONSTRAINT ck_inventory_reserved_le_on_hand CHECK (reserved <= on_hand),
    CONSTRAINT ck_inventory_price CHECK (unit_price >= 0 AND unit_cost >= 0)
) ENGINE=InnoDB;

CREATE TABLE stock_reservation (
    id          BIGINT       NOT NULL AUTO_INCREMENT,
    order_id    VARCHAR(24)  NOT NULL,
    sku         VARCHAR(64)  NOT NULL,
    quantity    INT          NOT NULL,
    status      VARCHAR(16)  NOT NULL,
    created_at  DATETIME(6)  NOT NULL,
    updated_at  DATETIME(6)  NOT NULL,
    PRIMARY KEY (id),
    -- one reservation line per SKU per order
    UNIQUE KEY uq_reservation_order_sku (order_id, sku),
    KEY idx_reservation_order_status (order_id, status),
    CONSTRAINT fk_reservation_sku FOREIGN KEY (sku) REFERENCES inventory_item (sku),
    CONSTRAINT ck_reservation_qty CHECK (quantity > 0)
) ENGINE=InnoDB;

CREATE TABLE sales_order (
    id               VARCHAR(24)   NOT NULL,
    customer_id      VARCHAR(24)   NOT NULL,
    idempotency_key  VARCHAR(64)   NULL,
    status           VARCHAR(16)   NOT NULL,
    failure_reason   VARCHAR(500)  NULL,
    currency         VARCHAR(3)    NOT NULL,
    total_price      DECIMAL(14,2) NOT NULL,
    version          BIGINT        NOT NULL DEFAULT 0,
    created_at       DATETIME(6)   NOT NULL,
    updated_at       DATETIME(6)   NOT NULL,
    PRIMARY KEY (id),
    -- MySQL allows many NULLs in a UNIQUE index: only requests that send a key are de-duplicated
    UNIQUE KEY uq_sales_order_idempotency (idempotency_key),
    KEY idx_sales_order_customer (customer_id, created_at),
    CONSTRAINT ck_sales_order_total CHECK (total_price >= 0)
) ENGINE=InnoDB;

CREATE TABLE sales_order_line (
    id          BIGINT        NOT NULL AUTO_INCREMENT,
    order_id    VARCHAR(24)   NOT NULL,
    line_no     INT           NOT NULL,
    sku         VARCHAR(64)   NOT NULL,
    product_id  VARCHAR(24)   NOT NULL,
    quantity    INT           NOT NULL,
    unit_price  DECIMAL(12,2) NOT NULL,
    line_total  DECIMAL(14,2) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_order_line_no (order_id, line_no),
    CONSTRAINT fk_order_line_order FOREIGN KEY (order_id) REFERENCES sales_order (id),
    CONSTRAINT ck_order_line_qty CHECK (quantity > 0)
) ENGINE=InnoDB;

-- append-only audit trail of the saga (steps and compensations)
CREATE TABLE saga_step (
    id          BIGINT       NOT NULL AUTO_INCREMENT,
    order_id    VARCHAR(24)  NOT NULL,
    step        VARCHAR(40)  NOT NULL,
    outcome     VARCHAR(24)  NOT NULL,
    detail      VARCHAR(500) NULL,
    created_at  DATETIME(6)  NOT NULL,
    PRIMARY KEY (id),
    KEY idx_saga_step_order (order_id, id)
) ENGINE=InnoDB;

-- transactional outbox: written in the same transaction as the order status change
CREATE TABLE outbox_event (
    id             BIGINT       NOT NULL AUTO_INCREMENT,
    event_id       VARCHAR(36)  NOT NULL,
    aggregate_id   VARCHAR(24)  NOT NULL,
    topic          VARCHAR(100) NOT NULL,
    partition_key  VARCHAR(64)  NOT NULL,
    payload        TEXT         NOT NULL,
    status         VARCHAR(16)  NOT NULL,
    attempts       INT          NOT NULL DEFAULT 0,
    last_error     VARCHAR(500) NULL,
    created_at     DATETIME(6)  NOT NULL,
    published_at   DATETIME(6)  NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_outbox_event_id (event_id),
    KEY idx_outbox_status_id (status, id),
    KEY idx_outbox_aggregate (aggregate_id)
) ENGINE=InnoDB;

CREATE TABLE purchase_order (
    id          BIGINT       NOT NULL AUTO_INCREMENT,
    supplier    VARCHAR(120) NOT NULL,
    status      VARCHAR(16)  NOT NULL,
    version     BIGINT       NOT NULL DEFAULT 0,
    created_at  DATETIME(6)  NOT NULL,
    updated_at  DATETIME(6)  NOT NULL,
    PRIMARY KEY (id),
    KEY idx_purchase_order_status (status)
) ENGINE=InnoDB;

CREATE TABLE purchase_order_line (
    id                BIGINT        NOT NULL AUTO_INCREMENT,
    purchase_order_id BIGINT        NOT NULL,
    sku               VARCHAR(64)   NOT NULL,
    quantity          INT           NOT NULL,
    unit_cost         DECIMAL(12,2) NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_po_line_po  FOREIGN KEY (purchase_order_id) REFERENCES purchase_order (id),
    CONSTRAINT fk_po_line_sku FOREIGN KEY (sku) REFERENCES inventory_item (sku),
    CONSTRAINT ck_po_line_qty CHECK (quantity > 0)
) ENGINE=InnoDB;
