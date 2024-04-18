/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.db2;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.apache.commons.collections4.MapUtils;

import com.google.auto.service.AutoService;

import java.util.Collections;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@AutoService(DataTypeConvertor.class)
public class Db2DataTypeConvertor implements DataTypeConvertor<String> {

    private static final String PRECISION = "precision";
    private static final String SCALE = "scale";
    private static final Integer DEFAULT_PRECISION = 38;
    private static final Integer DEFAULT_SCALE = 18;

    /* ============================ DB2 Data Types ===================== */
    private static final String DB2_BOOLEAN = "BOOLEAN";

    private static final String DB2_SMALLINT = "SMALLINT";
    private static final String DB2_INTEGER = "INTEGER";
    private static final String DB2_INT = "INT";
    private static final String DB2_BIGINT = "BIGINT";
    // exact
    private static final String DB2_DECIMAL = "DECIMAL";
    private static final String DB2_DEC = "DEC";
    private static final String DB2_NUMERIC = "NUMERIC";
    // float
    private static final String DB2_REAL = "REAL";
    private static final String DB2_FLOAT = "FLOAT";
    private static final String DB2_DOUBLE = "DOUBLE";
    private static final String DB2_DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String DB2_DECFLOAT = "DECFLOAT";
    // string
    private static final String DB2_CHAR = "CHAR";
    private static final String DB2_VARCHAR = "VARCHAR";
    private static final String DB2_LONG_VARCHAR = "LONG VARCHAR";
    private static final String DB2_CLOB = "CLOB";
    // graphic
    private static final String DB2_GRAPHIC = "GRAPHIC";
    private static final String DB2_VARGRAPHIC = "VARGRAPHIC";
    private static final String DB2_LONG_VARGRAPHIC = "LONG VARGRAPHIC";
    private static final String DB2_DBCLOB = "DBCLOB";

    // ---------------------------binary---------------------------
    private static final String DB2_BINARY = "BINARY";
    private static final String DB2_VARBINARY = "VARBINARY";

    // ------------------------------time-------------------------
    private static final String DB2_DATE = "DATE";
    private static final String DB2_TIME = "TIME";
    private static final String DB2_TIMESTAMP = "TIMESTAMP";

    // ------------------------------blob-------------------------
    private static final String DB2_BLOB = "BLOB";

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String field, String connectorDataType) {
        return toSeaTunnelType(field, connectorDataType, Collections.emptyMap());
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String field, String connectorDataType, Map<String, Object> dataTypeProperties) {
        checkNotNull(connectorDataType, "DB2 Type cannot be null for field: " + field);

        switch (connectorDataType) {
            case DB2_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case DB2_SMALLINT:
                return BasicType.SHORT_TYPE;
            case DB2_INT:
            case DB2_INTEGER:
                return BasicType.INT_TYPE;
            case DB2_BIGINT:
                return BasicType.LONG_TYPE;
            case DB2_DECIMAL:
                int precision =
                        MapUtils.getInteger(dataTypeProperties, PRECISION, DEFAULT_PRECISION);
                int scale = MapUtils.getInteger(dataTypeProperties, SCALE, DEFAULT_SCALE);
                return new DecimalType(precision, scale);
            case DB2_DEC:
            case DB2_NUMERIC:
            case DB2_REAL:
                return BasicType.FLOAT_TYPE;
            case DB2_FLOAT:
            case DB2_DOUBLE:
            case DB2_DOUBLE_PRECISION:
            case DB2_DECFLOAT:
                return BasicType.DOUBLE_TYPE;
            case DB2_CHAR:
            case DB2_VARCHAR:
            case DB2_LONG_VARCHAR:
            case DB2_CLOB:
            case DB2_GRAPHIC:
            case DB2_VARGRAPHIC:
            case DB2_LONG_VARGRAPHIC:
            case DB2_DBCLOB:
                return BasicType.STRING_TYPE;
            case DB2_BINARY:
            case DB2_VARBINARY:
            case DB2_BLOB:
                return PrimitiveByteArrayType.INSTANCE;
            case DB2_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case DB2_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case DB2_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.DB_2, connectorDataType, field);
        }
    }

    @Override
    public String toConnectorType(
            String field,
            SeaTunnelDataType<?> seaTunnelDataType,
            Map<String, Object> dataTypeProperties) {
        checkNotNull(seaTunnelDataType, "SeaTunnelDataType cannot be null for field: " + field);
        SqlType sqlType = seaTunnelDataType.getSqlType();
        switch (sqlType) {
            case SMALLINT:
                return DB2_SMALLINT;
            case INT:
                return DB2_INTEGER;
            case BIGINT:
                return DB2_BIGINT;
            case DECIMAL:
                return DB2_DECIMAL;
            case FLOAT:
                return DB2_REAL;
            case DOUBLE:
                return DB2_DOUBLE;
            case STRING:
                return DB2_VARCHAR; // Could also map to DB2_CHAR depending on use case
            case DATE:
                return DB2_DATE;
            case TIME:
                return DB2_TIME;
            case TIMESTAMP:
                return DB2_TIMESTAMP;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.DB_2, seaTunnelDataType.getSqlType().toString(), field);
        }
    }

    @Override
    public String getIdentity() {
        return DatabaseIdentifier.DB_2;
    }
}
