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

package org.apache.seatunnel.connectors.seatunnel.assertion.sink;

import org.apache.seatunnel.shade.com.google.common.base.Throwables;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertCatalogTableRule;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertFieldRule;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertRuleParser;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertTableRule;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.CATALOG_TABLE_RULES;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.FIELD_RULES;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.ROW_RULES;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.TABLE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertSinkOptions.RULES;

public class AssertSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportMultiTableSink {
    private final SeaTunnelRowType seaTunnelRowType;
    private final Map<String, List<AssertFieldRule>> assertFieldRules;
    private final Map<String, List<AssertFieldRule.AssertRule>> assertRowRules;
    private final AssertTableRule assertTableRule;
    private final Map<String, AssertCatalogTableRule> assertCatalogTableRule;
    private final String catalogTableName;
    private final CatalogTable catalogTable;

    public AssertSink(ReadonlyConfig pluginConfig, CatalogTable catalogTable) {
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        if (!pluginConfig.getOptional(RULES).isPresent()) {
            Throwables.throwIfUnchecked(new ConfigException.Missing(RULES.key()));
        }
        assertFieldRules = new ConcurrentHashMap<>();
        assertRowRules = new ConcurrentHashMap<>();
        assertCatalogTableRule = new ConcurrentHashMap<>();
        catalogTableName = catalogTable.getTablePath().getFullName();
        Config ruleConfig = ConfigFactory.parseMap(pluginConfig.get(RULES));
        if (ruleConfig.hasPath(ConnectorCommonOptions.TABLE_CONFIGS.key())) {
            List<? extends Config> tableConfigs =
                    ruleConfig.getConfigList(ConnectorCommonOptions.TABLE_CONFIGS.key());
            for (Config tableConfig : tableConfigs) {
                String tableName = tableConfig.getString(TABLE_PATH);
                initTableRule(catalogTable, tableConfig, tableName);
            }
        } else {
            String tableName = catalogTable.getTablePath().getFullName();
            initTableRule(catalogTable, ruleConfig, tableName);
        }

        if (ruleConfig.hasPath(ConnectorCommonOptions.TABLE_NAMES.key())) {
            assertTableRule =
                    new AssertTableRule(
                            ruleConfig.getStringList(ConnectorCommonOptions.TABLE_NAMES.key()));
        } else {
            assertTableRule = new AssertTableRule(new ArrayList<>());
        }

        if (assertRowRules.isEmpty()
                && assertFieldRules.isEmpty()
                && assertCatalogTableRule.isEmpty()
                && assertTableRule.getTableNames().isEmpty()) {
            Throwables.throwIfUnchecked(
                    new ConfigException.BadValue(
                            RULES.key(), "Assert rule config is empty, please add rule config."));
        }
        this.catalogTable = catalogTable;
    }

    private void initTableRule(CatalogTable catalogTable, Config tableConfig, String tableName) {
        List<? extends Config> rowConfigList;
        List<? extends Config> configList;
        if (tableConfig.hasPath(ROW_RULES)) {
            rowConfigList = tableConfig.getConfigList(ROW_RULES);
            assertRowRules.put(tableName, new AssertRuleParser().parseRowRules(rowConfigList));
        }
        if (tableConfig.hasPath(FIELD_RULES)) {
            configList = tableConfig.getConfigList(FIELD_RULES);
            assertFieldRules.put(tableName, new AssertRuleParser().parseRules(configList));
        }

        if (tableConfig.hasPath(CATALOG_TABLE_RULES)) {
            AssertCatalogTableRule catalogTableRule =
                    new AssertRuleParser()
                            .parseCatalogTableRule(tableConfig.getConfig(CATALOG_TABLE_RULES));
            if (tableName.equals(catalogTableName)) {
                catalogTableRule.checkRule(catalogTable);
            }
            assertCatalogTableRule.put(tableName, catalogTableRule);
        }
    }

    @Override
    public AssertSinkWriter createWriter(SinkWriter.Context context) {
        return new AssertSinkWriter(
                seaTunnelRowType,
                assertFieldRules,
                assertRowRules,
                assertTableRule,
                catalogTableName);
    }

    @Override
    public String getPluginName() {
        return "Assert";
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }
}
