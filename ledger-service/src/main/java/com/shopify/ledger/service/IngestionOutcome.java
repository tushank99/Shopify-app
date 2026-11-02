package com.shopify.ledger.service;

public enum IngestionOutcome {

    /** First time this eventId was seen: ledger_entry + all ledger_item rows were written. */
    INSERTED,

    /** eventId already in the ledger (redelivery, offset replay, relay crash after send). Nothing written. */
    DUPLICATE_EVENT,

    /** Same business order already ledgered under a different eventId (unique order_id + event_type). Nothing written. */
    DUPLICATE_ORDER
}