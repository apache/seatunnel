package org.apache.seatunnel.connectors.seatunnel.mongodb.serde;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.bson.Document;

public class DocumentRowDataDeserializer implements DocumentDeserializer<SeaTunnelRow> {

    private final String[] fieldNames;

    private final SeaTunnelDataType<?>[] fieldTypes;

    private final BsonToRowDataConverters bsonConverters;


    public DocumentRowDataDeserializer(String[] fieldNames, SeaTunnelDataType dataTypes) {
        if (fieldNames == null || fieldNames.length < 1) {
            throw new IllegalArgumentException("fieldName is empty");
        }

        this.bsonConverters = new BsonToRowDataConverters();
        this.fieldNames = fieldNames;
        this.fieldTypes = ((SeaTunnelRowType) dataTypes).getFieldTypes();
    }

    @Override
    public SeaTunnelRow deserialize(Document document) {
        SeaTunnelRow rowData = new SeaTunnelRow(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = this.fieldNames[i];
            Object o = document.get(fieldName);
            SeaTunnelDataType<?> fieldType = fieldTypes[i];
            rowData.setField(
                    i, bsonConverters.createConverter(fieldType).convert(null, o));
        }
        return rowData;
    }
}

