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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.schema;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.List;
import java.util.Set;

public class MongodbValueToTypeConvertor {

    public static SeaTunnelDataType<?> convertTypeFromValue(BsonValue bsonValue) {
        if (bsonValue == null || bsonValue.isNull()) {
            return BasicType.VOID_TYPE;
        }
        if (bsonValue.isString()) {
            return BasicType.STRING_TYPE;
        } else if (bsonValue.isBoolean()) {
            return BasicType.BOOLEAN_TYPE;
        } else if (bsonValue.isInt32()) {
            return BasicType.INT_TYPE;
        } else if (bsonValue.isInt64()) {
            return BasicType.LONG_TYPE;
        } else if (bsonValue.isDouble()) {
            return BasicType.DOUBLE_TYPE;
        } else if (bsonValue.isDecimal128()) {
            return BasicType.DOUBLE_TYPE;
        } else if (bsonValue.isDateTime()) {
            return LocalTimeType.LOCAL_DATE_TIME_TYPE;
        } else if (bsonValue.isBinary()) {
            return PrimitiveByteArrayType.INSTANCE;
        } else if (bsonValue.isArray()) {
            return parseArrayType(bsonValue.asArray());
        } else if (bsonValue.isDocument()) {
            return parseDocumentType(bsonValue.asDocument());
        } else if (bsonValue.isObjectId()) {
            return BasicType.STRING_TYPE;
        } else if (bsonValue.isSymbol()
                || bsonValue.isRegularExpression()
                || bsonValue.isJavaScript()
                || bsonValue.isJavaScriptWithScope()) {
            return BasicType.STRING_TYPE;
        } else {
            return BasicType.STRING_TYPE;
        }
    }

    private static SeaTunnelDataType<?> parseArrayType(BsonArray bsonArray) {
        if (bsonArray.isEmpty()) {
            return new ArrayType<>(List.class, BasicType.STRING_TYPE);
        }
        BsonValue first = bsonArray.get(0);
        SeaTunnelDataType<?> elementType = convertTypeFromValue(first);
        return new ArrayType<>(List.class, elementType);
    }

    private static SeaTunnelDataType<?> parseDocumentType(BsonDocument doc) {
        if (doc.isEmpty()) {
            return new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE);
        }
        // Get only one key-value pair
        final Set<String> keySet = doc.keySet();
        for (String key : keySet) {
            final BsonValue bsonValue = doc.get(key);
            SeaTunnelDataType<?> type = convertTypeFromValue(bsonValue);
            return new MapType<>(BasicType.STRING_TYPE, type);
        }
        return new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE);
    }
}
