package io.github.i1ich.mskei.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.i1ich.mskei.common.Topics;
import io.github.i1ich.mskei.it.support.Downstream;
import io.github.i1ich.mskei.it.support.Events;
import io.github.i1ich.mskei.it.support.TopicTail;
import io.github.i1ich.mskei.schema.OrderEvent;
import io.github.i1ich.mskei.schema.OrderOp;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

/** PostgreSQL row changes -> Debezium (Connect) -> erp.public.orders -> stream-processor -> delivery-service. */
class CdcOrdersIT extends AbstractIT {

    @Test
    void insertUpdateDeleteBecomeCreateUpdateDeleteEventsInOrderAndAreDelivered() throws Exception {
        String accountId = Events.accountId();
        long orderId;
        try (Connection c = ENV.jdbc()) {
            try (PreparedStatement ps = c.prepareStatement(
                    "insert into public.orders (account_id, status, amount, currency) values (?, 'NEW', 10.50, 'EUR')"
                            + " returning id")) {
                ps.setString(1, accountId);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    orderId = rs.getLong(1);
                }
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "update public.orders set status = 'PAID', amount = 11.75 where id = ?")) {
                ps.setLong(1, orderId);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement("delete from public.orders where id = ?")) {
                ps.setLong(1, orderId);
                ps.executeUpdate();
            }
        }

        try (TopicTail orders = ENV.tail(Topics.ORDER_V1)) {
            // Key = order id, so all three events are on one partition and stay in commit order.
            List<ConsumerRecord<byte[], byte[]>> records = orders.await(TopicTail.keyIs(Long.toString(orderId)), 3);
            assertThat(records).hasSize(3);
            assertThat(records).extracting(ConsumerRecord::partition).containsOnly(records.get(0).partition());

            List<OrderEvent> events = records.stream()
                    .map(r -> (OrderEvent) ENV.avroDeserializer().deserialize(Topics.ORDER_V1, r.value()))
                    .toList();
            assertThat(events).extracting(OrderEvent::getOp)
                    .containsExactly(OrderOp.CREATE, OrderOp.UPDATE, OrderOp.DELETE);
            assertThat(events).extracting(OrderEvent::getOrderId).containsOnly(orderId);
            assertThat(events).extracting(OrderEvent::getAccountId).containsOnly(accountId);

            assertThat(events.get(0).getStatus()).isEqualTo("NEW");
            assertThat(events.get(0).getAmount()).isEqualTo("10.50");
            assertThat(events.get(1).getStatus()).isEqualTo("PAID");
            assertThat(events.get(1).getAmount()).isEqualTo("11.75");
            // DELETE has no "after": the event carries the last known row from "before".
            assertThat(events.get(2).getStatus()).isEqualTo("PAID");
            assertThat(events.get(2).getAmount()).isEqualTo("11.75");
            assertThat(events.get(2).getCurrency()).isEqualTo("EUR");
        }

        List<Downstream.Call> calls = Downstream.awaitCalls(ENV.wireMock(), "/downstream/orders",
                b -> b.path("orderId").asLong(-1) == orderId, 3);
        assertThat(calls).extracting(c -> c.body().path("op").asText()).containsExactly("CREATE", "UPDATE", "DELETE");
        assertThat(calls).extracting(Downstream.Call::idempotencyKey).doesNotContainNull().doesNotHaveDuplicates();
    }
}
