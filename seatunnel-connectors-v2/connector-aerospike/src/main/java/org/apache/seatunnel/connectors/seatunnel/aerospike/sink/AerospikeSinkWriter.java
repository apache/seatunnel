package org.apache.seatunnel.connectors.seatunnel.aerospike.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.aerospike.config.AerospikeConfig;
import org.apache.seatunnel.connectors.seatunnel.aerospike.config.AerospikeDataType;
import org.apache.seatunnel.connectors.seatunnel.aerospike.config.DataFormatType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.connectors.seatunnel.aerospike.exception.AerospikeErrorCode;
import org.apache.seatunnel.connectors.seatunnel.aerospike.exception.AerospikeConnectorException;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AerospikeSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final SeaTunnelRowType seaTunnelRowType;
    private final ReadonlyConfig config;
    private final SerializationSchema serializationSchema;
    private final AerospikeClient aerospikeClient;
    private final WritePolicy writePolicy;
    private final AerospikeTypeConverter typeConverter;

    public AerospikeSinkWriter(SeaTunnelRowType seaTunnelRowType, ReadonlyConfig config) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.config = config;
        this.serializationSchema = new JsonSerializationSchema(seaTunnelRowType);
        this.aerospikeClient = buildClient();

        this.writePolicy = new WritePolicy();
        this.writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
        this.writePolicy.totalTimeout = config.get(AerospikeConfig.WRITE_TIMEOUT);
        this.writePolicy.socketTimeout = config.get(AerospikeConfig.WRITE_TIMEOUT);
        this.writePolicy.sleepBetweenRetries = 0;
        this.writePolicy.maxRetries = 0;
        this.typeConverter = new AerospikeTypeConverter(seaTunnelRowType, config);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        try {
            String data = new String(serializationSchema.serialize(element));
            String keyField = config.get(AerospikeConfig.KEY_FIELD);
            String key = element.getField(seaTunnelRowType.indexOf(keyField)).toString();

            Key aerospikeKey =
                    new Key(
                            config.get(AerospikeConfig.NAMESPACE),
                            config.get(AerospikeConfig.SET),
                            key);

            String formatValue = config.get(AerospikeConfig.DATA_FORMAT).toLowerCase();
            DataFormatType formatType = DataFormatType.fromString(formatValue);

            switch (formatType) {
                case MAP:
                    Map<String, Object> dataMap =
                            JSON.parseObject(data, new TypeReference<Map<String, Object>>() {});
                    Bin dataBin = new Bin(config.get(AerospikeConfig.BIN_NAME), dataMap);
                    aerospikeClient.put(writePolicy, aerospikeKey, dataBin);
                    break;

                case STRING:
                    Bin stringBin = new Bin(config.get(AerospikeConfig.BIN_NAME), data);
                    aerospikeClient.put(writePolicy, aerospikeKey, stringBin);
                    break;

                case KEY_VALUE:
                    Map<String, Object> fieldsMap =
                            JSON.parseObject(data, new TypeReference<Map<String, Object>>() {});
                    List<Bin> bins = new ArrayList<>();
                    for (Map.Entry<String, Object> entry : fieldsMap.entrySet()) {
                        AerospikeDataType dataType = typeConverter.getFieldType(entry.getKey());
                        Object value = convertValue(entry.getValue(), dataType);
                        bins.add(new Bin(entry.getKey(), value));
                    }
                    aerospikeClient.put(writePolicy, aerospikeKey, bins.toArray(new Bin[0]));
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported data format type: " + formatType);
            }
        } catch (Exception e) {
            throw new AerospikeConnectorException(
                AerospikeErrorCode.WRITER_OPERATION_FAILED,
                "Failed to write record",
                e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (Objects.nonNull(aerospikeClient)) {
                aerospikeClient.close();
            }
        } catch (Exception e) {
            throw new AerospikeConnectorException(
                AerospikeErrorCode.WRITER_CLOSE_FAILED,
                "Failed to close writer",
                e);
        }
    }

    private AerospikeClient buildClient() {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.user = config.get(AerospikeConfig.USERNAME);
        clientPolicy.password = config.get(AerospikeConfig.PASSWORD);
        clientPolicy.timeout = config.get(AerospikeConfig.WRITE_TIMEOUT);
        clientPolicy.maxConnsPerNode = 300;

        return new AerospikeClient(
                clientPolicy, config.get(AerospikeConfig.HOST), config.get(AerospikeConfig.PORT));
    }

    private Object convertValue(Object value, AerospikeDataType dataType) {
        if (value == null) {
            return null;
        }

        switch (dataType) {
            case STRING:
                return value.toString();
            case INTEGER:
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                return Integer.parseInt(value.toString());
            case LONG:
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                return Long.parseLong(value.toString());
            case FLOAT:
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                }
                return Float.parseFloat(value.toString());
            case DOUBLE:
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                return Double.parseDouble(value.toString());
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                }
                return Boolean.parseBoolean(value.toString());
            case MAP:
                return value;
            case LIST:
                if (value instanceof Iterable) {
                    return value;
                }
                throw new IllegalArgumentException(
                        "Expected List type but got: " + value.getClass());
            default:
                throw new IllegalArgumentException("Unsupported AEROSPIKE data type: " + dataType);
        }
    }
}
