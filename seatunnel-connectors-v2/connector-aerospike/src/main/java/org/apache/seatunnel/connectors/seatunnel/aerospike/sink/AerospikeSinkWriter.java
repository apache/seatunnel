package org.apache.seatunnel.connectors.seatunnel.aerospike.sink;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.aerospike.config.AerospikeDataType;
import org.apache.seatunnel.connectors.seatunnel.aerospike.config.AerospikeParameters;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
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
    private final AerospikeParameters aerospikeParameters;
    private final SerializationSchema serializationSchema;
    private final AerospikeClient aerospikeClient;
    private final WritePolicy writePolicy;
    private final AerospikeTypeConverter typeConverter;

    public AerospikeSinkWriter(
            SeaTunnelRowType seaTunnelRowType, AerospikeParameters aerospikeParameters) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.aerospikeParameters = aerospikeParameters;
        this.serializationSchema = new JsonSerializationSchema(seaTunnelRowType);
        this.aerospikeClient = aerospikeParameters.buildClient();

        // Create write policy locally
        this.writePolicy = new WritePolicy();
        this.writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
        this.writePolicy.totalTimeout = 200;
        this.writePolicy.socketTimeout = 200;
        this.writePolicy.sleepBetweenRetries = 0;
        this.writePolicy.maxRetries = 0;
        this.typeConverter = new AerospikeTypeConverter(seaTunnelRowType, aerospikeParameters);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        String data = new String(serializationSchema.serialize(element));
        String keyField = aerospikeParameters.getKeyField();
        String key = element.getField(seaTunnelRowType.indexOf(keyField)).toString();

        Key aerospikeKey =
                new Key(aerospikeParameters.getNamespace(), aerospikeParameters.getSet(), key);

        switch (aerospikeParameters.getDataFormatType()) {
            case MAP_FORMAT:
                // 将整个JSON数据解析为Map结构
                Map<String, Object> dataMap =
                        JSON.parseObject(data, new TypeReference<Map<String, Object>>() {});
                Bin dataBin = new Bin(aerospikeParameters.getBinName(), dataMap);
                aerospikeClient.put(writePolicy, aerospikeKey, dataBin);
                break;

            case STRING_FORMAT:
                // 直接使用字符串格式
                Bin stringBin = new Bin(aerospikeParameters.getBinName(), data);
                aerospikeClient.put(writePolicy, aerospikeKey, stringBin);
                break;

            case KEY_VALUE_FORMAT:
                // 每个字段作为单独的bin
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
                throw new IllegalArgumentException(
                        "Unsupported data format type: " + aerospikeParameters.getDataFormatType());
        }
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

    public void close() throws IOException {
        if (Objects.nonNull(aerospikeClient)) {
            aerospikeClient.close();
        }
    }
}
