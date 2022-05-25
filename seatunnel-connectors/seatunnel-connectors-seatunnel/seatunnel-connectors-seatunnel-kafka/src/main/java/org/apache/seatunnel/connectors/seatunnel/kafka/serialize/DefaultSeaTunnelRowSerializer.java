package org.apache.seatunnel.connectors.seatunnel.kafka.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.kafka.clients.producer.ProducerRecord;

public class DefaultSeaTunnelRowSerializer implements SeaTunnelRowSerializer<String, SeaTunnelRow> {

    private final String topic;

    public DefaultSeaTunnelRowSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<String, SeaTunnelRow> serializeRow(SeaTunnelRow row) {
        return new ProducerRecord<>(topic, null, row);
    }
}
