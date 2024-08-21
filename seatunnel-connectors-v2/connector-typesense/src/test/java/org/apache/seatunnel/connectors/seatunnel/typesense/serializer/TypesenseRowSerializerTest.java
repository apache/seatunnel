package org.apache.seatunnel.connectors.seatunnel.typesense.serializer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.typesense.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.typesense.dto.CollectionInfo;
import org.apache.seatunnel.connectors.seatunnel.typesense.serialize.sink.TypesenseRowSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;

public class TypesenseRowSerializerTest {
    @Test
    public void testSerializeUpsert() {
        String collection = "test";
        String primaryKey = "id";
        Map<String, Object> confMap = new HashMap<>();
        confMap.put(SinkConfig.COLLECTION.key(), collection);
        confMap.put(SinkConfig.PRIMARY_KEYS.key(), Arrays.asList(primaryKey));

        ReadonlyConfig pluginConf = ReadonlyConfig.fromMap(confMap);
        CollectionInfo collectionInfo = new CollectionInfo(collection,pluginConf);
        SeaTunnelRowType schema =
                new SeaTunnelRowType(
                        new String[] {primaryKey, "name"},
                        new SeaTunnelDataType[] {STRING_TYPE, STRING_TYPE});
        TypesenseRowSerializer typesenseRowSerializer = new TypesenseRowSerializer(collectionInfo,schema);
        String id = "0001";
        String name = "jack";
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, name});
        row.setRowKind(RowKind.UPDATE_AFTER);
        Assertions.assertEquals(typesenseRowSerializer.serializeRowForDelete(row),id);
        row.setRowKind(RowKind.INSERT);
        String data = "{\"name\":\"jack\",\"id\":\"0001\"}";
        Assertions.assertEquals(typesenseRowSerializer.serializeRow(row),data);
    }
}
