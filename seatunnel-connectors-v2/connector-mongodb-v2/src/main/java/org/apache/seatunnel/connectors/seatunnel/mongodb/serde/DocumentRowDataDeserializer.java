/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
            rowData.setField(i, bsonConverters.createConverter(fieldType).convert(null, o));
        }
        return rowData;
    }
}
