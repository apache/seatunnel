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
import org.apache.seatunnel.connectors.seatunnel.mongodb.exception.MongodbConnectorException;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import static org.apache.seatunnel.api.table.type.SqlType.STRING;
import static org.apache.seatunnel.common.exception.CommonErrorCode.ILLEGAL_ARGUMENT;
import static org.apache.seatunnel.common.exception.CommonErrorCode.UNSUPPORTED_OPERATION;

public class DocumentRowDataDeserializer implements DocumentDeserializer<SeaTunnelRow> {

    private final String[] fieldNames;

    private final SeaTunnelDataType<?>[] fieldTypes;

    private final BsonToRowDataConverters bsonConverters;

    private final boolean flatSyncString;

    public DocumentRowDataDeserializer(
            String[] fieldNames, SeaTunnelDataType<?> dataTypes, boolean flatSyncString) {
        if (fieldNames == null || fieldNames.length < 1) {
            throw new MongodbConnectorException(ILLEGAL_ARGUMENT, "fieldName is empty");
        }
        this.bsonConverters = new BsonToRowDataConverters();
        this.fieldNames = fieldNames;
        this.fieldTypes = ((SeaTunnelRowType) dataTypes).getFieldTypes();
        this.flatSyncString = flatSyncString;
    }

    @Override
    public SeaTunnelRow deserialize(BsonDocument bsonDocument) {
        if (flatSyncString) {
            if (fieldNames.length != 1 && fieldTypes[0].getSqlType() != STRING) {
                throw new MongodbConnectorException(
                        UNSUPPORTED_OPERATION,
                        "By utilizing flatSyncString, only one field attribute value can be set, and the field type must be a String. This operation will perform a string mapping on a single MongoDB data entry.");
            }
            SeaTunnelRow rowData = new SeaTunnelRow(fieldNames.length);
            rowData.setField(
                    0, bsonConverters.createConverter(fieldTypes[0]).convert(bsonDocument));
            return rowData;
        }
        SeaTunnelRow rowData = new SeaTunnelRow(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = this.fieldNames[i];
            BsonValue o = bsonDocument.get(fieldName);
            SeaTunnelDataType<?> fieldType = fieldTypes[i];
            rowData.setField(i, bsonConverters.createConverter(fieldType).convert(o));
        }
        return rowData;
    }
}
