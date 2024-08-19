package org.apache.seatunnel.connectors.seatunnel.typesense.serialize.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.typesense.dto.CollectionInfo;
import org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.typesense.serialize.KeyExtractor;
import org.apache.seatunnel.connectors.seatunnel.typesense.serialize.sink.collection.CollectionSerializer;
import org.apache.seatunnel.connectors.seatunnel.typesense.serialize.sink.collection.CollectionSerializerFactory;

import org.apache.commons.lang3.StringUtils;

import java.time.temporal.Temporal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class TypesenseRowSerializer implements SeaTunnelRowSerializer {

    private final CollectionSerializer collectionSerializer;

    private final SeaTunnelRowType seaTunnelRowType;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Function<SeaTunnelRow, String> keyExtractor;

    public TypesenseRowSerializer(
            CollectionInfo collectionInfo, SeaTunnelRowType seaTunnelRowType) {
        this.collectionSerializer =
                CollectionSerializerFactory.getIndexSerializer(collectionInfo.getCollection());
        this.seaTunnelRowType = seaTunnelRowType;
        this.keyExtractor =
                KeyExtractor.createKeyExtractor(
                        seaTunnelRowType,
                        collectionInfo.getPrimaryKeys(),
                        collectionInfo.getKeyDelimiter());
    }

    // TODO 拼接写入Typesense 语句
    @Override
    public String serializeRow(SeaTunnelRow row) {
        switch (row.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                return serializeUpsert(row);
            case UPDATE_BEFORE:
            case DELETE:
                //                return returnserializeDelete(row);
            default:
                throw new TypesenseConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        "Unsupported write row kind: " + row.getRowKind());
        }
        //        return null;
    }

    private String serializeUpsert(SeaTunnelRow row) {
        String key = keyExtractor.apply(row);
        Map<String, Object> document = toDocumentMap(row, seaTunnelRowType);
        if (StringUtils.isNotBlank(key)) {
            document.put("id", key);
        }
        String documentStr;
        try {
            documentStr = objectMapper.writeValueAsString(document);
        } catch (JsonProcessingException e) {
            throw CommonError.jsonOperationError("Typesense", "document:" + document.toString(), e);
        }
        return documentStr;
    }

    private Map<String, Object> toDocumentMap(SeaTunnelRow row, SeaTunnelRowType rowType) {
        String[] fieldNames = rowType.getFieldNames();
        Map<String, Object> doc = new HashMap<>(fieldNames.length);
        Object[] fields = row.getFields();
        for (int i = 0; i < fieldNames.length; i++) {
            Object value = fields[i];
            if (value == null) {
            } else if (value instanceof SeaTunnelRow) {
                doc.put(
                        fieldNames[i],
                        toDocumentMap(
                                (SeaTunnelRow) value, (SeaTunnelRowType) rowType.getFieldType(i)));
            } else {
                doc.put(fieldNames[i], convertValue(value));
            }
        }
        return doc;
    }

    private Object convertValue(Object value) {
        if (value instanceof Temporal) {
            // jackson not support jdk8 new time api
            return value.toString();
        } else if (value instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                ((Map) value).put(entry.getKey(), convertValue(entry.getValue()));
            }
            return value;
        } else if (value instanceof List) {
            for (int i = 0; i < ((List) value).size(); i++) {
                ((List) value).set(i, convertValue(((List) value).get(i)));
            }
            return value;
        } else {
            return value;
        }
    }
}
