package org.apache.seatunnel.format.json.kafka.connect;

import java.io.IOException;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.NullNode;

public class KafkaConnectDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {

    private final SeaTunnelRowType physicalRowType;
    private transient JsonConverter keyConverter;
    private transient JsonConverter valueConverter;

    /** Object mapper for parsing the JSON. */
    private final ObjectMapper objectMapper = new ObjectMapper();

    private transient String topic;
    public KafkaConnectDeserializationSchema(SeaTunnelRowType physicalRowType, boolean keySchemaEnable, boolean valueSchemaEnable, String topic) {
        this.physicalRowType = physicalRowType;
        //Init kafka connect record key converter
        keyConverter = new JsonConverter();
        keyConverter.configure(
            Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, keySchemaEnable),
            true);

        // Init kafka connect record value converter
        valueConverter = new JsonConverter();
        valueConverter.configure(
            Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, valueSchemaEnable),
            false);

        this.topic = topic;

    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        SchemaAndValue schemaAndValue = valueConverter.toConnectData(topic, message);
        schemaAndValue.schema().fields();
        // TODO list
        return null;
    }


    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.physicalRowType;
    }
}
