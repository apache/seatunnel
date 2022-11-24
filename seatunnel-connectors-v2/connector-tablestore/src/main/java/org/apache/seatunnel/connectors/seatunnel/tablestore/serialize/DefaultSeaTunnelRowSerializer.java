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

package org.apache.seatunnel.connectors.seatunnel.tablestore.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.tablestore.config.TablestoreOptions;
import org.apache.seatunnel.connectors.seatunnel.tablestore.exception.TablestoreConnectorException;

import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.Condition;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RowExistenceExpectation;
import com.alicloud.openservices.tablestore.model.RowPutChange;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DefaultSeaTunnelRowSerializer implements SeaTunnelRowSerializer {

    private final SeaTunnelRowType seaTunnelRowType;
    private final TablestoreOptions tablestoreOptions;

    public DefaultSeaTunnelRowSerializer(SeaTunnelRowType seaTunnelRowType, TablestoreOptions tablestoreOptions) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.tablestoreOptions = tablestoreOptions;
    }

    @Override
    public RowPutChange serialize(SeaTunnelRow seaTunnelRow) {

        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        List<Column> columns = new ArrayList<>(seaTunnelRow.getFields().length - tablestoreOptions.getPrimaryKeys().size());
        Arrays.stream(seaTunnelRowType.getFieldNames()).forEach(fieldName -> {
            Object field = seaTunnelRow.getField(seaTunnelRowType.indexOf(fieldName));
            int index = seaTunnelRowType.indexOf(fieldName);
            if (tablestoreOptions.getPrimaryKeys().contains(fieldName)) {
                primaryKeyBuilder.addPrimaryKeyColumn(
                    this.convertPrimaryKeyColumn(fieldName, field,
                        this.convertPrimaryKeyType(seaTunnelRowType.getFieldType(index))));
            } else {
                columns.add(this.convertColumn(fieldName, field,
                    this.convertColumnType(seaTunnelRowType.getFieldType(index))));
            }
        });
        RowPutChange rowPutChange = new RowPutChange(tablestoreOptions.getTable(), primaryKeyBuilder.build());
        rowPutChange.setCondition(new Condition(RowExistenceExpectation.IGNORE));
        columns.forEach(rowPutChange::addColumn);

        return rowPutChange;
    }

    private ColumnType convertColumnType(SeaTunnelDataType<?> seaTunnelDataType) {
        switch (seaTunnelDataType.getSqlType()) {
            case INT:
            case TINYINT:
            case SMALLINT:
            case BIGINT:
                return ColumnType.INTEGER;
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return ColumnType.DOUBLE;
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return ColumnType.STRING;
            case BOOLEAN:
                return ColumnType.BOOLEAN;
            case BYTES:
                return ColumnType.BINARY;
            default:
                throw new TablestoreConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported columnType: " + seaTunnelDataType);
        }
    }

    private PrimaryKeyType convertPrimaryKeyType(SeaTunnelDataType<?> seaTunnelDataType) {
        switch (seaTunnelDataType.getSqlType()) {
            case INT:
            case TINYINT:
            case SMALLINT:
            case BIGINT:
                return PrimaryKeyType.INTEGER;
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case BOOLEAN:
                return PrimaryKeyType.STRING;
            case BYTES:
                return PrimaryKeyType.BINARY;
            default:
                throw new TablestoreConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported primaryKeyType: " + seaTunnelDataType);
        }
    }

    private Column convertColumn(String columnName, Object value, ColumnType columnType) {
        if (value == null) {
            return null;
        }
        switch (columnType) {
            case STRING:
                return new Column(columnName, ColumnValue.fromString(String.valueOf(value)));
            case INTEGER:
                return new Column(columnName, ColumnValue.fromLong((long) value));
            case BOOLEAN:
                return new Column(columnName, ColumnValue.fromBoolean((boolean) value));
            case DOUBLE:
                return new Column(columnName, ColumnValue.fromDouble((Double) value));
            case BINARY:
                return new Column(columnName, ColumnValue.fromBinary((byte[]) value));
            default:
                throw new TablestoreConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported columnType: " + columnType);
        }
    }

    private PrimaryKeyColumn convertPrimaryKeyColumn(String columnName, Object value, PrimaryKeyType primaryKeyType) {
        if (value == null) {
            return null;
        }
        switch (primaryKeyType) {
            case STRING:
                return new PrimaryKeyColumn(columnName, PrimaryKeyValue.fromString(String.valueOf(value)));
            case INTEGER:
                return new PrimaryKeyColumn(columnName, PrimaryKeyValue.fromLong((long) value));
            case BINARY:
                return new PrimaryKeyColumn(columnName, PrimaryKeyValue.fromBinary((byte[]) value));
            default:
                throw new TablestoreConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported primaryKeyType: " + primaryKeyType);
        }
    }

}
