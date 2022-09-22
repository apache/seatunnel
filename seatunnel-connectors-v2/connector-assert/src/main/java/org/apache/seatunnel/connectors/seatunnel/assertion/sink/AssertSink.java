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

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertFieldRule;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertRowRule;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertRuleParser;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;

import com.google.auto.service.AutoService;
import com.google.common.base.Throwables;
import org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.util.List;

@AutoService(SeaTunnelSink.class)
public class AssertSink extends AbstractSimpleSink<SeaTunnelRow, Void> {
    private static final String RULES = "rules";
    private static final String ROW_RULES = "row_rules";
    private static final String FIELD_RULES = "field_rules";
    private SeaTunnelRowType seaTunnelRowType;
    private List<AssertFieldRule> assertFieldRules;
    private List<AssertFieldRule.AssertRule> assertRowRules;

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) throws IOException {
        return new AssertSinkWriter(seaTunnelRowType, assertFieldRules, assertRowRules);
    }

    @Override
    public void prepare(Config pluginConfig) {
        if (!pluginConfig.hasPath(RULES)) {
            Throwables.propagateIfPossible(new ConfigException.Missing(RULES));
        }
        Config ruleConfig = pluginConfig.getConfig(RULES);
        List<? extends Config> rowConfigList = ruleConfig.getConfigList(ROW_RULES);
        List<? extends Config> configList = ruleConfig.getConfigList(FIELD_RULES);
        if (CollectionUtils.isEmpty(configList) && CollectionUtils.isEmpty(rowConfigList)) {
            Throwables.propagateIfPossible(new ConfigException.BadValue(RULES, "Assert rule config is empty, please add rule config."));
        }
        assertFieldRules = new AssertRuleParser().parseRules(configList);
        assertRowRules = new AssertRuleParser().parseRowRules(rowConfigList);
    }

    @Override
    public String getPluginName() {
        return "Assert";
    }
}
