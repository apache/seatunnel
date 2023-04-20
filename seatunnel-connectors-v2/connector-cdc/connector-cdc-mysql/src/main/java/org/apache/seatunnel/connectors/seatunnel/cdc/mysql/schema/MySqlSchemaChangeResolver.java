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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.schema;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.cdc.base.schema.SchemaChangeResolver;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.schema.ddl.ConnectTableChangeSerializer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.schema.ddl.MySqlAntlrDdlParser;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.List;

@Slf4j
public class MySqlSchemaChangeResolver implements SchemaChangeResolver {
    private final ConnectTableChangeSerializer tableChangeSerializer =
            new ConnectTableChangeSerializer();

    public MySqlSchemaChangeResolver(MySqlValueConverters converters) {}

    @Override
    public SchemaChangeEvent resolve(SourceRecord record, SeaTunnelDataType dataType) {
        Struct value = (Struct) record.value();
        String ddl = value.getString(HistoryRecord.Fields.DDL_STATEMENTS);
        List<Struct> tableChangesStruct =
                (List<Struct>) value.get(HistoryRecord.Fields.TABLE_CHANGES);
        if (tableChangesStruct.size() != 1) {
            log.debug("Unsupported parse ddl: {}", ddl);
            return null;
        }

        TableChanges tableChanges = tableChangeSerializer.deserialize(tableChangesStruct, false);
        TablePath tablePath = SourceRecordUtils.getTablePath(record);
        SchemaChangeEvent schemaChangeEvent =
                resolveTableChanges(ddl, tableChanges, tablePath, dataType);
        if (schemaChangeEvent == null) {
            log.debug("Unsupported parse ddl: {}", ddl);
            return null;
        }
        return schemaChangeEvent;
    }

    private SchemaChangeEvent resolveTableChanges(
            String ddl,
            TableChanges tableChanges,
            TablePath tablePath,
            SeaTunnelDataType dataType) {
        Iterator<TableChanges.TableChange> iterator = tableChanges.iterator();
        iterator.hasNext();
        TableChanges.TableChange tableChange = iterator.next();

        TableId tableId = tableChange.getId();
        TableChanges.TableChangeType tableChangeType = tableChange.getType();
        Table table = tableChange.getTable();
        MySqlAntlrDdlParser ddlParser = new MySqlAntlrDdlParser(";", false, null);
        ddlParser.setCurrentSchema(tablePath.getDatabaseName());
        switch (tableChangeType) {
            case ALTER:
                SchemaChanges schemaChanges = ddlParser.getSchemaChanges();
                schemaChanges.reset();
                ddlParser.parse(ddl, table.edit());
                List<SchemaChangeEvent> events = schemaChanges.getEvents();
                return events.get(0);
            case CREATE:
            case DROP:
            default:
                return null;
        }
    }
}
