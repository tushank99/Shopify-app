package com.shopify.erp.it;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** I-1: the Flyway migration applies on an empty MySQL 8 and its CHECK constraints really protect the data. */
class SchemaIntegrationTest extends AbstractIntegrationTest {

    @Test
    void flywayAppliedTheMigrationSuccessfully() {
        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*) FROM flyway_schema_history WHERE version = '1' AND success = 1", Long.class))
                .isEqualTo(1L);
    }

    @Test
    void theDatabaseRejectsReservingMoreThanOnHandEvenIfTheCodeHadABug() {
        String sku = uniqueSku("CHK");
        createItem(sku, 5, "1.00", 0, 0);

        assertThatThrownBy(() -> jdbc.update("UPDATE inventory_item SET reserved = 6 WHERE sku = ?", sku))
                .isInstanceOf(DataAccessException.class)
                .hasMessageContaining("ck_inventory_reserved_le_on_hand");

        assertThatThrownBy(() -> jdbc.update("UPDATE inventory_item SET on_hand = -1 WHERE sku = ?", sku))
                .isInstanceOf(DataAccessException.class);

        assertThat(item(sku).get("reserved")).isEqualTo(0);
    }

    @Test
    void aReservationForAnUnknownSkuViolatesTheForeignKey() {
        assertThatThrownBy(() -> jdbc.update(
                "INSERT INTO stock_reservation (order_id, sku, quantity, status, created_at, updated_at) "
                        + "VALUES ('aaaaaaaaaaaaaaaaaaaaaaaa', 'NO-SUCH-SKU', 1, 'RESERVED', NOW(6), NOW(6))"))
                .isInstanceOf(DataAccessException.class)
                .hasMessageContaining("fk_reservation_sku");
    }
}
