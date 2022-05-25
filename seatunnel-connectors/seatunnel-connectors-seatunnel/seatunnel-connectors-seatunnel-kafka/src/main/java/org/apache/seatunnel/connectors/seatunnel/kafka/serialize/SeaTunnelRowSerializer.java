package org.apache.seatunnel.connectors.seatunnel.kafka.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.kafka.clients.producer.ProducerRecord;

public interface SeaTunnelRowSerializer<K, V> {

    /**
     * Serialize the {@link SeaTunnelRow} to a Kafka {@link ProducerRecord}.
     *
     * @param row seatunnel row
     * @return kafka record.
     */
    ProducerRecord<K, V> serializeRow(SeaTunnelRow row);
}
