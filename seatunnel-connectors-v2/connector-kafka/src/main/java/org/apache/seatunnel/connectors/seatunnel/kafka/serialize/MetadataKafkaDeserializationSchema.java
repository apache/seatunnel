package org.apache.seatunnel.connectors.seatunnel.kafka.serialize;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class MetadataKafkaDeserializationSchema implements KafkaDeserializationSchema<SeaTunnelRow> {

    private final DeserializationSchema<SeaTunnelRow> keyDeserialization;

    private final DeserializationSchema<SeaTunnelRow> valueDeserialization;

    private final boolean hasMetadata;

    private final MetadataConverter[] metadataConverters;

    private final int dataArity;

    private final int[] keyProjection;

    private final int[] valueProjection;

    public MetadataKafkaDeserializationSchema(DeserializationSchema<SeaTunnelRow> keyDeserialization,
                                              DeserializationSchema<SeaTunnelRow> valueDeserialization,
                                              boolean hasMetadata, MetadataConverter[] metadataConverters,
                                              int dataArity, int[] keyProjection, int[] valueProjection) {
        this.keyDeserialization = keyDeserialization;
        this.valueDeserialization = valueDeserialization;
        this.hasMetadata = hasMetadata;
        this.metadataConverters = metadataConverters;
        this.dataArity = dataArity;
        this.keyProjection = keyProjection;
        this.valueProjection = valueProjection;
    }

    @Override
    public SeaTunnelRow deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        return null;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<SeaTunnelRow> out) throws Exception {
        if (keyDeserialization == null && !hasMetadata) {
            valueDeserialization.deserialize(record.value(), out);
            return;
        }
        SeaTunnelRow keyDeserialize = null;
        if (keyDeserialization != null) {
            keyDeserialize = keyDeserialization.deserialize(record.key());
        }
        SeaTunnelRow valueDeserialize = null;
        if (record.value() == null) {
            out.collect(null);
            return;
        } else {
            valueDeserialize = valueDeserialization.deserialize(record.value());
        }
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(dataArity + metadataConverters.length);
        if (keyDeserialize != null) {
            for (int keyPos = 0; keyPos < keyProjection.length; keyPos++) {
                seaTunnelRow.setField(keyProjection[keyPos], keyDeserialize.getField(keyPos));
            }
        }
        if (valueDeserialize != null) {
            for (int valuePos = 0; valuePos < valueProjection.length; valuePos++) {
                seaTunnelRow.setField(valueProjection[valuePos], valueDeserialize.getField(valuePos));
            }
        }
        for (int metadataPos = 0; metadataPos < metadataConverters.length; metadataPos++) {
            seaTunnelRow.setField(
                dataArity + metadataPos,
                metadataConverters[metadataPos].read(record));
        }
    }

    interface MetadataConverter extends Serializable {
        Object read(ConsumerRecord<?, ?> record);
    }

    public enum ReadableMetadata {
        TOPIC(
            "topic",
            BasicType.STRING_TYPE,
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(ConsumerRecord<?, ?> record) {
                    return String.valueOf(record.topic());
                }
            }),

        PARTITION(
            "partition",
            BasicType.INT_TYPE,
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(ConsumerRecord<?, ?> record) {
                    return record.partition();
                }
            }),

        HEADERS(
            "headers",
            new MapType<>(BasicType.STRING_TYPE, BasicType.BYTE_TYPE),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(ConsumerRecord<?, ?> record) {
                    final Map<String, byte[]> map = new HashMap<>();
                    for (Header header : record.headers()) {
                        map.put(String.valueOf(header.key()), header.value());
                    }
                    return map;
                }
            }),

        LEADER_EPOCH(
            "leader-epoch",
            BasicType.INT_TYPE,
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(ConsumerRecord<?, ?> record) {
                    return record.leaderEpoch().orElse(null);
                }
            }),

        OFFSET(
            "offset",
            BasicType.LONG_TYPE,
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(ConsumerRecord<?, ?> record) {
                    return record.offset();
                }
            }),

        TIMESTAMP(
            "timestamp",
            LocalTimeType.LOCAL_DATE_TIME_TYPE,
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(ConsumerRecord<?, ?> record) {
                    return new Timestamp(record.timestamp()).toLocalDateTime();
                }
            }),

        TIMESTAMP_TYPE(
            "timestamp-type",
            BasicType.STRING_TYPE,
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(ConsumerRecord<?, ?> record) {
                    return record.timestampType().toString();
                }
            });

        final String key;

        final SeaTunnelDataType dataType;

        final MetadataConverter converter;

        ReadableMetadata(String key, SeaTunnelDataType dataType, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
