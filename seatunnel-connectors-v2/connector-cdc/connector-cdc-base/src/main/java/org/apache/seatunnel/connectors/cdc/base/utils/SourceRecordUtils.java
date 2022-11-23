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

package org.apache.seatunnel.connectors.cdc.base.utils;

import static io.debezium.connector.AbstractSourceInfo.DATABASE_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import io.debezium.data.Envelope;
import io.debezium.document.DocumentReader;
import io.debezium.relational.TableId;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

/** Utility class to deal record. */
public class SourceRecordUtils {

    private SourceRecordUtils() {}

    public static final String SCHEMA_CHANGE_EVENT_KEY_NAME =
            "io.debezium.connector.mysql.SchemaChangeKey";
    private static final DocumentReader DOCUMENT_READER = DocumentReader.defaultReader();

    /** Converts a {@link ResultSet} row to an array of Objects. */
    public static Object[] rowToArray(ResultSet rs, int size) throws SQLException {
        final Object[] row = new Object[size];
        for (int i = 0; i < size; i++) {
            row[i] = rs.getObject(i + 1);
        }
        return row;
    }

    /**
     * Return the timestamp when the change event is produced in MySQL.
     *
     * <p>The field `source.ts_ms` in {@link SourceRecord} data struct is the time when the change
     * event is operated in MySQL.
     */
    public static Long getMessageTimestamp(SourceRecord record) {
        Schema schema = record.valueSchema();
        Struct value = (Struct) record.value();
        if (schema.field(Envelope.FieldName.SOURCE) == null) {
            return null;
        }

        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        if (source.schema().field(Envelope.FieldName.TIMESTAMP) == null) {
            return null;
        }

        return source.getInt64(Envelope.FieldName.TIMESTAMP);
    }

    /**
     * The field `ts_ms` in {@link SourceRecord} data struct is the time when the record fetched by
     * debezium reader, use it as the process time in Source.
     */
    public static Long getFetchTimestamp(SourceRecord record) {
        Schema schema = record.valueSchema();
        Struct value = (Struct) record.value();
        if (schema.field(Envelope.FieldName.TIMESTAMP) == null) {
            return null;
        }
        return value.getInt64(Envelope.FieldName.TIMESTAMP);
    }

    public static boolean isSchemaChangeEvent(SourceRecord sourceRecord) {
        Schema keySchema = sourceRecord.keySchema();
        return keySchema != null && SCHEMA_CHANGE_EVENT_KEY_NAME.equalsIgnoreCase(keySchema.name());
    }

    public static boolean isDataChangeRecord(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        Struct value = (Struct) record.value();
        return valueSchema.field(Envelope.FieldName.OPERATION) != null
                && value.getString(Envelope.FieldName.OPERATION) != null;
    }

    public static TableId getTableId(SourceRecord dataRecord) {
        Struct value = (Struct) dataRecord.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        String dbName = source.getString(DATABASE_NAME_KEY);
        // Oracle need schemaName
        String schemaName = getSchemaName(source);
        String tableName = source.getString(TABLE_NAME_KEY);
        return new TableId(dbName, schemaName, tableName);
    }

    public static String getSchemaName(Struct source) {
        if (source.schema().fields().stream().anyMatch(r -> SCHEMA_NAME_KEY.equals(r.name()))) {
            return source.getString(SCHEMA_NAME_KEY);
        }
        return null;
    }

    public static Object[] getSplitKey(
        SeaTunnelRowType splitBoundaryType, SourceRecord dataRecord, SchemaNameAdjuster nameAdjuster) {
        // the split key field contains single field now
        String splitFieldName = nameAdjuster.adjust(splitBoundaryType.getFieldNames()[0]);
        Struct key = (Struct) dataRecord.key();
        return new Object[] {key.get(splitFieldName)};
    }

    /** Returns the specific key contains in the split key range or not. */
    public static boolean splitKeyRangeContains(
            Object[] key, Object[] splitKeyStart, Object[] splitKeyEnd) {
        // for all range
        if (splitKeyStart == null && splitKeyEnd == null) {
            return true;
        }
        // first split
        if (splitKeyStart == null) {
            int[] upperBoundRes = new int[key.length];
            for (int i = 0; i < key.length; i++) {
                upperBoundRes[i] = compareObjects(key[i], splitKeyEnd[i]);
            }
            return Arrays.stream(upperBoundRes).anyMatch(value -> value < 0)
                    && Arrays.stream(upperBoundRes).allMatch(value -> value <= 0);
        }
        // last split
        else if (splitKeyEnd == null) {
            int[] lowerBoundRes = new int[key.length];
            for (int i = 0; i < key.length; i++) {
                lowerBoundRes[i] = compareObjects(key[i], splitKeyStart[i]);
            }
            return Arrays.stream(lowerBoundRes).allMatch(value -> value >= 0);
        }
        // other split
        else {
            int[] lowerBoundRes = new int[key.length];
            int[] upperBoundRes = new int[key.length];
            for (int i = 0; i < key.length; i++) {
                lowerBoundRes[i] = compareObjects(key[i], splitKeyStart[i]);
                upperBoundRes[i] = compareObjects(key[i], splitKeyEnd[i]);
            }
            return Arrays.stream(lowerBoundRes).anyMatch(value -> value >= 0)
                    && Arrays.stream(upperBoundRes).anyMatch(value -> value < 0)
                            && Arrays.stream(upperBoundRes).allMatch(value -> value <= 0);
        }
    }

    @SuppressWarnings("unchecked")
    private static int compareObjects(Object o1, Object o2) {
        if (o1 instanceof Comparable && o1.getClass().equals(o2.getClass())) {
            return ((Comparable) o1).compareTo(o2);
        } else if (isNumericObject(o1) && isNumericObject(o2)) {
            return toBigDecimal(o1).compareTo(toBigDecimal(o2));
        } else {
            return o1.toString().compareTo(o2.toString());
        }
    }

    private static boolean isNumericObject(Object obj) {
        return obj instanceof Byte
                || obj instanceof Short
                || obj instanceof Integer
                || obj instanceof Long
                || obj instanceof Float
                || obj instanceof Double
                || obj instanceof BigInteger
                || obj instanceof BigDecimal;
    }

    private static BigDecimal toBigDecimal(Object numericObj) {
        return new BigDecimal(numericObj.toString());
    }
}
