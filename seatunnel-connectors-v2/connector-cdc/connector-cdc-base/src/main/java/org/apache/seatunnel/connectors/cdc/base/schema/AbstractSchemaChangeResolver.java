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

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import com.google.common.collect.Lists;
import io.debezium.relational.history.HistoryRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public abstract class AbstractSchemaChangeResolver implements SchemaChangeResolver {

    protected static final List<String> SUPPORT_DDL = Lists.newArrayList("ALTER TABLE");

    protected JdbcSourceConfig jdbcSourceConfig;

    public AbstractSchemaChangeResolver(JdbcSourceConfig jdbcSourceConfig) {
        this.jdbcSourceConfig = jdbcSourceConfig;
    }

    @Override
    public boolean support(SourceRecord record) {
        String ddl = SourceRecordUtils.getDdl(record);
        Struct value = (Struct) record.value();
        List<Struct> tableChanges = value.getArray(HistoryRecord.Fields.TABLE_CHANGES);
        if (tableChanges == null || tableChanges.isEmpty()) {
            log.warn("Ignoring statement for non-captured table {}", ddl);
            return false;
        }
        return StringUtils.isNotBlank(ddl)
                && SUPPORT_DDL.stream()
                        .map(String::toUpperCase)
                        .anyMatch(prefix -> ddl.toUpperCase().contains(prefix));
    }
}
