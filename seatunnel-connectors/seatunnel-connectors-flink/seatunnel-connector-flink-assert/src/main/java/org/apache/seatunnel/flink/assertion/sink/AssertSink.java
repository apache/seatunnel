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

package org.apache.seatunnel.flink.assertion.sink;

import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.BaseFlinkSink;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.assertion.AssertExecutor;
import org.apache.seatunnel.flink.assertion.rule.AssertFieldRule;
import org.apache.seatunnel.flink.assertion.rule.AssertRuleParser;
import org.apache.seatunnel.flink.batch.FlinkBatchSink;
import org.apache.seatunnel.flink.stream.FlinkStreamSink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * A flink sink plugin which can assert illegal data by user defined rules Refer to https://github.com/apache/incubator-seatunnel/issues/1912
 */
@AutoService(BaseFlinkSink.class)
public class AssertSink implements FlinkBatchSink, FlinkStreamSink {

    //The assertion executor
    private static final AssertExecutor ASSERT_EXECUTOR = new AssertExecutor();
    //User defined rules used to assert illegal data
    private List<AssertFieldRule> assertFieldRules;
    private static final String RULES = "rules";
    private Config config;
    private List<? extends Config> configList;

    @SneakyThrows
    @Override
    public void outputBatch(FlinkEnvironment env, DataSet<Row> inDataSet) {
        try {
            inDataSet.collect().forEach(row ->
                ASSERT_EXECUTOR
                    .fail(row, assertFieldRules)
                    .ifPresent(failRule -> {
                        throw new IllegalStateException("row :" + row + " fail rule: " + failRule);
                    }));
        } catch (Exception ex) {
            throw new RuntimeException("AssertSink execute failed", ex);
        }
    }

    @Override
    public void outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        dataStream.map(row -> {
            ASSERT_EXECUTOR
                .fail(row, assertFieldRules)
                .ifPresent(failRule -> {
                    throw new IllegalStateException("row :" + row + "field name of the fail rule: " + failRule.getFieldName());
                });
            return null;
        });
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        if (config.hasPath(RULES)) {
            configList = this.config.getConfigList(RULES);
            if (CollectionUtils.isNotEmpty(configList)) {
                return CheckResult.success();
            }
        }
        return CheckResult.error("There is no assert-rule defined in AssertSink plugin");
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        AssertRuleParser assertRuleParser = new AssertRuleParser();
        assertFieldRules = assertRuleParser.parseRules(configList);
    }

    @Override
    public void close() {

    }

    @Override
    public String getPluginName() {
        return "AssertSink";
    }
}
