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

package org.apache.seatunnel.flink.source;

import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.stream.FlinkStreamSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;

public class FakeSource implements FlinkStreamSource {

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
    public void prepare(FlinkEnvironment prepareEnv) {

    }

    @Override
    public DataStream<Row> getData(FlinkEnvironment env) {
        Random random = new Random();
        return env.getStreamExecutionEnvironment().fromCollection(
                Arrays.stream(NAME_ARRAY).map(n -> Row.of(n, random.nextInt(AGE_LIMIT)))
                .collect(Collectors.toList()), getRowTypeInfo());
    }

    private RowTypeInfo getRowTypeInfo() {
        TypeInformation[] types = Lists.newArrayList(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO).toArray(new TypeInformation[0]);
        String[] filedNames = Lists.newArrayList("name", "age").toArray(new String[0]);
        return new RowTypeInfo(types, filedNames);
    }

}
