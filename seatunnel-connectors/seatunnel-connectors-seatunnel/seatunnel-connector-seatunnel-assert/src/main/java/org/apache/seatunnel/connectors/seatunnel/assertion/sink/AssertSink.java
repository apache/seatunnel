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

import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertFieldRule;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertRuleParser;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;

import com.google.auto.service.AutoService;
import com.google.common.base.Throwables;
import org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.util.List;

@AutoService(SeaTunnelSink.class)
public class AssertSink implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void> {
    private static final String RULES = "rules";
    private SeaTunnelContext seaTunnelContext;
    private SeaTunnelRowType seaTunnelRowType;
    private List<AssertFieldRule> assertFieldRules;

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context) throws IOException {
        return new AssertSinkWriter(seaTunnelRowType, assertFieldRules);
    }

    @Override
    public void prepare(Config pluginConfig) {
        if (!pluginConfig.hasPath(RULES)) {
            Throwables.propagateIfPossible(new ConfigException.Missing(RULES));
        }

        List<? extends Config> configList = pluginConfig.getConfigList(RULES);
        if (CollectionUtils.isEmpty(configList)) {
            Throwables.propagateIfPossible(new ConfigException.BadValue(RULES, "Assert rule config is empty, please add rule config."));
        }
        assertFieldRules = new AssertRuleParser().parseRules(configList);
    }

    @Override
    public SeaTunnelContext getSeaTunnelContext() {
        return seaTunnelContext;
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        this.seaTunnelContext = seaTunnelContext;
    }

    @Override
    public String getPluginName() {
        return "AssertSink";
    }
}
