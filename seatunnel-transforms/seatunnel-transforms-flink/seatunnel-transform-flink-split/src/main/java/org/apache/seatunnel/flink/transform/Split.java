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

package org.apache.seatunnel.flink.transform;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchTransform;
import org.apache.seatunnel.flink.stream.FlinkStreamTransform;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.List;

public class Split implements FlinkStreamTransform, FlinkBatchTransform {

    private Config config;

    private static final String SEPARATOR = "separator";
    private static final String FIELDS = "fields";
    private static final String NAME = "name";

    private String separator = ",";

    private String name = "split";

    private int num;

    private List<String> fields;

    private RowTypeInfo rowTypeInfo;

    @Override
    public DataSet<Row> processBatch(FlinkEnvironment env, DataSet<Row> data) {
        return data;
    }

    @Override
    public DataStream<Row> processStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        return dataStream;
    }

    @Override
    public void registerFunction(FlinkEnvironment flinkEnvironment) {
        if (flinkEnvironment.isStreaming()) {
            flinkEnvironment
                    .getStreamTableEnvironment()
                    .registerFunction(name, new ScalarSplit(rowTypeInfo, num, separator));
        } else {
            flinkEnvironment
                    .getBatchTableEnvironment()
                    .registerFunction(name, new ScalarSplit(rowTypeInfo, num, separator));
        }
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
        return CheckConfigUtil.checkAllExists(config, FIELDS);
    }

    @Override
    public void prepare(FlinkEnvironment prepareEnv) {
        fields = config.getStringList(FIELDS);
        num = fields.size();
        if (config.hasPath(SEPARATOR)) {
            separator = config.getString(SEPARATOR);
        }
        if (config.hasPath(NAME)) {
            name = config.getString(NAME);
        }
        TypeInformation<?>[] types = new TypeInformation[fields.size()];
        for (int i = 0; i < types.length; i++) {
            types[i] = Types.STRING();
        }
        rowTypeInfo = new RowTypeInfo(types, fields.toArray(new String[0]));
    }

    @Override
    public String getPluginName() {
        return "split";
    }
}
