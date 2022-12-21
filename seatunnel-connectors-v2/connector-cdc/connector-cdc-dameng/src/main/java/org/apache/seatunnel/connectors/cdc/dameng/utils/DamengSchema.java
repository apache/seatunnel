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

package org.apache.seatunnel.connectors.cdc.dameng.utils;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DamengSchema {
    private final Map<TableId, TableChanges.TableChange> schemasByTableId = new HashMap<>();

    public TableChanges.TableChange getTableSchema(JdbcConnection jdbc, TableId tableId) {
        // read schema from cache first
        TableChanges.TableChange schema = schemasByTableId.get(tableId);
        if (schema == null) {
            try {
                List<Column> columns = DamengConncetionUtils.queryColumns(jdbc, tableId);
                List<String> primaryKeyNames = DamengConncetionUtils.queryPrimaryKeyNames(jdbc, tableId);
                Table table = Table.editor()
                    .tableId(tableId)
                    .setColumns(columns)
                    .setPrimaryKeyNames(primaryKeyNames)
                    .create();
                schema = new TableChanges.TableChange(TableChanges.TableChangeType.CREATE, table);
                schemasByTableId.put(tableId, schema);
            } catch (SQLException e) {
                throw new SeaTunnelRuntimeException(CommonErrorCode.SQL_OPERATION_FAILED, e);
            }
        }
        return schema;
    }
}
