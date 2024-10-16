package org.apache.seatunnel.connectors.pinecone.utils;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.BufferUtils;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.pinecone.proto.Vector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.table.type.VectorType.VECTOR_FLOAT_TYPE;
import static org.apache.seatunnel.api.table.type.VectorType.VECTOR_SPARSE_FLOAT_TYPE;

public class ConverterUtils {
    public static SeaTunnelRow convertToSeatunnelRow(TableSchema tableSchema, Vector vector) {
        SeaTunnelRowType typeInfo = tableSchema.toPhysicalRowDataType();
        Object[] fields = new Object[typeInfo.getTotalFields()];
        List<String> fieldNames =
                Arrays.stream(typeInfo.getFieldNames()).collect(Collectors.toList());

        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            if (fieldNames.get(fieldIndex).equals("id")) {
                fields[fieldIndex] = vector.getId();
            } else if (fieldNames.get(fieldIndex).equals("meta")) {
                Struct meta = vector.getMetadata();
                JsonObject data = new JsonObject();
                for (Map.Entry<String, Value> entry : meta.getFieldsMap().entrySet()) {
                    data.add(entry.getKey(), convertValueToJsonElement(entry.getValue()));
                }
                fields[fieldIndex] = data;
            } else if (typeInfo.getFieldType(fieldIndex).equals(VECTOR_FLOAT_TYPE)) {
                List<Float> floats = vector.getValuesList();
                // Convert List<Float> to Float[]
                Float[] floatArray = floats.toArray(new Float[0]);
                fields[fieldIndex] = BufferUtils.toByteBuffer(floatArray);
            } else if (typeInfo.getFieldType(fieldIndex).equals(VECTOR_SPARSE_FLOAT_TYPE)) {
                // Convert SparseVector to a ByteBuffer
                Map<Long, Float> sparseMap = new HashMap<>();
                int count = vector.getSparseValues().getIndicesCount();
                for (int i = 0; i < count; i++) {
                    long index = vector.getSparseValues().getIndices(i);
                    float value = vector.getSparseValues().getValues(i);
                    sparseMap.put(index, value);
                }

                fields[fieldIndex] = sparseMap;
            }
        }

        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
        seaTunnelRow.setRowKind(RowKind.INSERT);
        return seaTunnelRow;
    }

    private static JsonElement convertValueToJsonElement(Value value) {
        Gson gson = new Gson();
        switch (value.getKindCase()) {
            case NULL_VALUE:
                return gson.toJsonTree(null); // Null value
            case NUMBER_VALUE:
                return gson.toJsonTree(value.getNumberValue()); // Double value
            case STRING_VALUE:
                return gson.toJsonTree(value.getStringValue()); // String value
            case BOOL_VALUE:
                return gson.toJsonTree(value.getBoolValue()); // Boolean value
            case STRUCT_VALUE:
                // Convert Struct to a JsonObject
                JsonObject structJson = new JsonObject();
                value.getStructValue()
                        .getFieldsMap()
                        .forEach((k, v) -> structJson.add(k, convertValueToJsonElement(v)));
                return structJson;
            case LIST_VALUE:
                // Convert List to a JsonArray
                return gson.toJsonTree(
                        value.getListValue().getValuesList().stream()
                                .map(ConverterUtils::convertValueToJsonElement)
                                .toArray());
            default:
                return gson.toJsonTree(null); // Default or unsupported case
        }
    }
}
