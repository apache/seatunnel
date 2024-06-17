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

package org.apache.seatunnel.connectors.cdc.base.schema;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.source.SourceRecord;

import com.google.common.collect.Lists;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

import java.util.List;

public abstract class AbstractSchemaChangeResolver implements SchemaChangeResolver {

    protected static final List<String> SUPPORT_DDL = Lists.newArrayList("ALTER TABLE");

    protected JdbcSourceConfig jdbcSourceConfig;

    public AbstractSchemaChangeResolver(JdbcSourceConfig jdbcSourceConfig) {
        this.jdbcSourceConfig = jdbcSourceConfig;
    }

    @Override
    public boolean support(SourceRecord record) {
        RelationalTableFilters tableFilters =
                jdbcSourceConfig.getDbzConnectorConfig().getTableFilters();
        Tables.TableFilter tableFilter = tableFilters.dataCollectionFilter();
        TablePath tablePath = SourceRecordUtils.getTablePath(record);
        String ddl = SourceRecordUtils.getDdl(record);
        return StringUtils.isNotEmpty(tablePath.getTableName())
                && tableFilter.isIncluded(toTableId(tablePath))
                && StringUtils.isNotBlank(ddl)
                && SUPPORT_DDL.stream()
                        .map(String::toUpperCase)
                        .anyMatch(prefix -> ddl.toUpperCase().contains(prefix));
    }

    private TableId toTableId(TablePath tablePath) {
        return new TableId(
                tablePath.getDatabaseName(), tablePath.getSchemaName(), tablePath.getTableName());
    }
}
