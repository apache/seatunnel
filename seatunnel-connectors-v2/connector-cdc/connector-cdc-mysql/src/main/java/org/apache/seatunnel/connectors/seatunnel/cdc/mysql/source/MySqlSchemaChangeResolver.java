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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.schema.AbstractSchemaChangeResolver;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.parser.CustomMySqlAntlrDdlParser;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import io.debezium.relational.ddl.DdlParser;

import java.util.List;

public class MySqlSchemaChangeResolver extends AbstractSchemaChangeResolver {

    public MySqlSchemaChangeResolver(SourceConfig.Factory<JdbcSourceConfig> sourceConfigFactory) {
        super(sourceConfigFactory.create(0));
    }

    @Override
    protected DdlParser createDdlParser(TablePath tablePath) {
        return new CustomMySqlAntlrDdlParser(
                tablePath, this.jdbcSourceConfig.getDbzConnectorConfig());
    }

    @Override
    protected List<AlterTableColumnEvent> getAndClearParsedEvents() {
        return ((CustomMySqlAntlrDdlParser) ddlParser).getAndClearParsedEvents();
    }

    @Override
    protected String getSourceDialectName() {
        return DatabaseIdentifier.MYSQL;
    }
}
