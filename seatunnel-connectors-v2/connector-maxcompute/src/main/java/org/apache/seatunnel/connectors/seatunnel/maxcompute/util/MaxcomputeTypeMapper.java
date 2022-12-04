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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.util;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.aliyun.odps.Column;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MaxcomputeTypeMapper implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(MaxcomputeTypeMapper.class);

    // ============================data types=====================

    private static final String MAXCOMPUTE_UNKNOWN = "UNKNOWN";
    private static final String MAXCOMPUTE_BOOLEAN = "BOOLEAN";

    // -------------------------number----------------------------
    private static final String MAXCOMPUTE_TINYINT = "TINYINT";
    private static final String MAXCOMPUTE_SMALLINT = "SMALLINT";
    private static final String MAXCOMPUTE_INT = "INT";
    private static final String MAXCOMPUTE_BIGINT = "BIGINT";

    private static final String MAXCOMPUTE_FLOAT = "FLOAT";

    private static final String MAXCOMPUTE_DOUBLE = "DOUBLE";
    private static final String MAXCOMPUTE_DECIMAL = "DECIMAL32";


    // -------------------------string----------------------------

    private static final String MAXCOMPUTE_VARCHAR = "VARCHAR";
    private static final String MAXCOMPUTE_CHAR = "CHAR";
    private static final String MAXCOMPUTE_STRING = "STRING";


    // ------------------------------time-------------------------

    private static final String MAXCOMPUTE_DATE = "DATE";
    private static final String MAXCOMPUTE_DATETIME = "DATETIME";
    private static final String MAXCOMPUTE_TIMESTAMP = "TIMESTAMP";


    // ------------------------------blob-------------------------

    private static final String MAXCOMPUTE_BINARY = "BINARY";
    private static final int PRECISION = 20;

    private static SeaTunnelDataType<?> mapping(List<Column> columnSchemaList, int colIndex) throws SQLException {
        String maxcomputeType = columnSchemaList.get(colIndex).getTypeInfo().getTypeName().toUpperCase();
        switch (maxcomputeType) {
            case MAXCOMPUTE_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case MAXCOMPUTE_TINYINT:
            case MAXCOMPUTE_SMALLINT:
            case MAXCOMPUTE_INT:
                return BasicType.INT_TYPE;
            case MAXCOMPUTE_BIGINT:
                return BasicType.LONG_TYPE;
            case MAXCOMPUTE_DECIMAL:
                return new DecimalType(PRECISION, 0);
            case MAXCOMPUTE_FLOAT:
                return BasicType.FLOAT_TYPE;
            case MAXCOMPUTE_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case MAXCOMPUTE_VARCHAR:
            case MAXCOMPUTE_CHAR:
            case MAXCOMPUTE_STRING:
                return BasicType.STRING_TYPE;
            case MAXCOMPUTE_DATE:
            case MAXCOMPUTE_DATETIME:
            case MAXCOMPUTE_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case MAXCOMPUTE_BINARY:
                return PrimitiveByteArrayType.INSTANCE;
            //Doesn't support yet
            case MAXCOMPUTE_UNKNOWN:
            default:
                throw new UnsupportedOperationException(
                    String.format(
                        "Doesn't support Maxcompute type '%s' .",
                        maxcomputeType));
        }
    }

    public static SeaTunnelRowType getSeaTunnelRowType(Config pluginConfig) {
        Table table = MaxcomputeUtil.getTable(pluginConfig);
        TableSchema tableSchema = table.getSchema();
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {
            for (int i = 0; i < tableSchema.getColumns().size(); i++) {
                fieldNames.add(tableSchema.getColumns().get(i).getName());
                seaTunnelDataTypes.add(mapping(tableSchema.getColumns(), i));
            }
        } catch (SQLException e) {
            LOG.warn("get row type info exception.", e);
            return null;
        }
        return new SeaTunnelRowType(fieldNames.toArray(new String[fieldNames.size()]), seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[seaTunnelDataTypes.size()]));
    }

    public static SeaTunnelRow getSeaTunnelRowData(Record rs, SeaTunnelRowType typeInfo) throws SQLException {

        List<Object> fields = new ArrayList<>();
        SeaTunnelDataType<?>[] seaTunnelDataTypes = typeInfo.getFieldTypes();
        for (int i = 0; i < seaTunnelDataTypes.length; i++) {
            Object seatunnelField;
            SeaTunnelDataType<?> seaTunnelDataType = seaTunnelDataTypes[i];
            if (null == rs.get(i)) {
                seatunnelField = null;
            } else if (BasicType.BOOLEAN_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getBoolean(i);
            } else if (BasicType.BYTE_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getBytes(i);
            } else if (BasicType.SHORT_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getBigint(i);
            } else if (BasicType.INT_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.get(i);
            } else if (BasicType.LONG_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getBigint(i);
            } else if (seaTunnelDataType instanceof DecimalType) {
                Object value = rs.getDecimal(i);
                seatunnelField = value instanceof BigInteger ?
                    new BigDecimal((BigInteger) value, 0)
                    : value;
            } else if (BasicType.FLOAT_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getDouble(i);
            } else if (BasicType.DOUBLE_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getDouble(i);
            } else if (BasicType.STRING_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getString(i);
            } else {
                throw new IllegalStateException("Unexpected value: " + seaTunnelDataType);
            }
            fields.add(seatunnelField);
        }
        return new SeaTunnelRow(fields.toArray());
    }

    public static Record getRecord(SeaTunnelRow seaTunnelRow, SeaTunnelRowType typeInfo, TableTunnel.UploadSession session, TableSchema tableSchema) {
        Record record = session.newRecord();
        String[] fieldNames = typeInfo.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = fieldNames[i];
            if (tableSchema.containsColumn(fieldName)) {
                record.set(fieldName, seaTunnelRow.getField(i));
            }
        }
        return record;
    }
}
