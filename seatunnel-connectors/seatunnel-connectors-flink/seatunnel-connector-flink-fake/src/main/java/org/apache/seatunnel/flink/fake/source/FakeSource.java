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

package org.apache.seatunnel.flink.fake.source;

import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;

public class FakeSource implements FlinkBatchSource {

    private static final String[] NAME_ARRAY = new String[]{"Gary", "Ricky Huo", "Kid Xiong"};
    private Config config;
    private static final int AGE_LIMIT = 100;

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
        return CheckResult.success();
    }

    @Override
    public String getPluginName() {
        return "FakeSource";
    }

    @Override
    public DataSet<Row> getData(FlinkEnvironment env) {
        Random random = new Random();
        return env.getBatchTableEnvironment().toDataSet(
            env.getBatchTableEnvironment().fromValues(
                DataTypes.ROW(DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("age", DataTypes.INT())),
                Arrays.stream(NAME_ARRAY).map(n -> Row.of(n, random.nextInt(AGE_LIMIT)))
                    .collect(Collectors.toList())), Row.class);
    }

}
