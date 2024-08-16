package org.apache.seatunnel.connectors.seatunnel.typesense.serialize.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.typesense.dto.CollectionInfo;
import org.apache.seatunnel.connectors.seatunnel.typesense.serialize.KeyExtractor;
import org.apache.seatunnel.connectors.seatunnel.typesense.serialize.sink.collection.CollectionSerializer;
import org.apache.seatunnel.connectors.seatunnel.typesense.serialize.sink.collection.CollectionSerializerFactory;

import java.util.function.Function;

public class TypesenseRowSerializer implements SeaTunnelRowSerializer {

    private final CollectionSerializer collectionSerializer;

    private final SeaTunnelRowType seaTunnelRowType;

    private final Function<SeaTunnelRow, String> keyExtractor;


    public TypesenseRowSerializer(
            CollectionInfo collectionInfo,
            SeaTunnelRowType seaTunnelRowType) {
        this.collectionSerializer =
                CollectionSerializerFactory.getIndexSerializer(collectionInfo.getCollection());
        this.seaTunnelRowType = seaTunnelRowType;
        this.keyExtractor =
                KeyExtractor.createKeyExtractor(
                        seaTunnelRowType, collectionInfo.getPrimaryKeys()
                        , collectionInfo.getKeyDelimiter());
    }

    // TODO 拼接写入Typesense 语句
    @Override
    public String serializeRow(SeaTunnelRow row) {
        return null;
    }



}
