package com.shopify.erp.orders;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Outbox relay: mark PUBLISHED only after the broker acks; on failure stop the batch and keep the row PENDING. */
@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    @Mock
    OutboxEventRepository outbox;
    @Mock
    KafkaTemplate<String, String> kafka;

    OutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new OutboxPublisher(outbox, kafka, mock(PlatformTransactionManager.class), 50, 3, 1000);
    }

    private static OutboxEvent event(String id) {
        return new OutboxEvent(id, "aaaaaaaaaaaaaaaaaaaaaaaa", "order.placed", "customer-1", "{\"eventId\":\"" + id + "\"}");
    }

    @Test
    @SuppressWarnings("unchecked")
    void publishesWithTheEventIdHeaderAndMarksRowsPublishedAfterTheAck() {
        OutboxEvent a = event("e-1");
        OutboxEvent b = event("e-2");
        when(outbox.lockByStatus(any(), any())).thenReturn(List.of(a, b));
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));

        publisher.publishPending();

        assertThat(a.getStatus()).isEqualTo(OutboxEvent.Status.PUBLISHED);
        assertThat(b.getStatus()).isEqualTo(OutboxEvent.Status.PUBLISHED);

        ArgumentCaptor<ProducerRecord<String, String>> sent = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafka, times(2)).send(sent.capture());
        ProducerRecord<String, String> first = sent.getAllValues().get(0);
        assertThat(first.topic()).isEqualTo("order.placed");
        assertThat(first.key()).isEqualTo("customer-1");
        assertThat(new String(first.headers().lastHeader("event-id").value(), StandardCharsets.UTF_8)).isEqualTo("e-1");
    }

    @Test
    @SuppressWarnings("unchecked")
    void aBrokerFailureKeepsTheRowPendingAndStopsTheBatch() {
        OutboxEvent a = event("e-1");
        OutboxEvent b = event("e-2");
        OutboxEvent c = event("e-3");
        when(outbox.lockByStatus(any(), any())).thenReturn(List.of(a, b, c));
        when(kafka.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        publisher.publishPending();

        assertThat(a.getStatus()).isEqualTo(OutboxEvent.Status.PUBLISHED);
        assertThat(b.getStatus()).isEqualTo(OutboxEvent.Status.PENDING);
        assertThat(b.getAttempts()).isEqualTo(1);
        assertThat(c.getAttempts()).isZero();               // never tried: ordering is preserved
        verify(kafka, times(2)).send(any(ProducerRecord.class));
    }

    @Test
    void aDatabaseErrorDuringThePollIsLoggedNotThrown() {
        when(outbox.lockByStatus(any(), any())).thenThrow(new IllegalStateException("db down"));

        assertThatCode(publisher::publishPending).doesNotThrowAnyException();
    }
}
