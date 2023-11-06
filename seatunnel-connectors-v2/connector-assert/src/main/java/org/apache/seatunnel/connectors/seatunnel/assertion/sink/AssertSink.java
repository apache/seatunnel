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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertCatalogTableRule;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertFieldRule;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertRuleParser;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertTableRule;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import org.apache.commons.collections4.CollectionUtils;

import com.google.common.base.Throwables;

import java.util.ArrayList;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.CATALOG_TABLE_RULES;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.FIELD_RULES;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.ROW_RULES;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.RULES;

public class AssertSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportMultiTableSink {
    private SeaTunnelRowType seaTunnelRowType;
    private List<AssertFieldRule> assertFieldRules;
    private List<AssertFieldRule.AssertRule> assertRowRules;
    private final AssertTableRule assertTableRule;
    private AssertCatalogTableRule assertCatalogTableRule;

    public AssertSink(ReadonlyConfig pluginConfig, CatalogTable catalogTable) {
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        if (!pluginConfig.getOptional(RULES).isPresent()) {
            Throwables.propagateIfPossible(new ConfigException.Missing(RULES.key()));
        }
        Config ruleConfig = ConfigFactory.parseMap(pluginConfig.get(RULES));
        List<? extends Config> rowConfigList = null;
        List<? extends Config> configList = null;
        if (ruleConfig.hasPath(ROW_RULES)) {
            rowConfigList = ruleConfig.getConfigList(ROW_RULES);
            assertRowRules = new AssertRuleParser().parseRowRules(rowConfigList);
        }
        if (ruleConfig.hasPath(FIELD_RULES)) {
            configList = ruleConfig.getConfigList(FIELD_RULES);
            assertFieldRules = new AssertRuleParser().parseRules(configList);
        }

        if (ruleConfig.hasPath(CATALOG_TABLE_RULES)) {
            assertCatalogTableRule =
                    new AssertRuleParser()
                            .parseCatalogTableRule(ruleConfig.getConfig(CATALOG_TABLE_RULES));
            assertCatalogTableRule.checkRule(catalogTable);
        }

        if (ruleConfig.hasPath(CatalogOptions.TABLE_NAMES.key())) {
            assertTableRule =
                    new AssertTableRule(ruleConfig.getStringList(CatalogOptions.TABLE_NAMES.key()));
        } else {
            assertTableRule = new AssertTableRule(new ArrayList<>());
        }

        if (CollectionUtils.isEmpty(configList)
                && CollectionUtils.isEmpty(rowConfigList)
                && assertCatalogTableRule == null
                && assertTableRule.getTableNames().isEmpty()) {
            Throwables.propagateIfPossible(
                    new ConfigException.BadValue(
                            RULES.key(), "Assert rule config is empty, please add rule config."));
        }
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return seaTunnelRowType;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) {
        return new AssertSinkWriter(
                seaTunnelRowType, assertFieldRules, assertRowRules, assertTableRule);
    }

    @Override
    public String getPluginName() {
        return "Assert";
    }
}
