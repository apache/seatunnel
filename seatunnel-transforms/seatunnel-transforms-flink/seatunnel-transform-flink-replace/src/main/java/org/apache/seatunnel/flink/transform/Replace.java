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

import com.sun.xml.internal.org.jvnet.fastinfoset.sax.FastInfosetReader;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchTransform;
import org.apache.seatunnel.flink.stream.FlinkStreamTransform;
import org.apache.seatunnel.flink.util.TableUtil;
import org.apache.seatunnel.plugin.Plugin;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.prefs.BackingStoreException;

import static org.apache.flink.table.api.Expressions.call;

public class Replace implements FlinkStreamTransform, FlinkBatchTransform {

    private Config config;

    private static final String PATTERN = "pattern";
    private static final String REPLACEMENT = "replacement";
    private static final String ISREGEX = "is_regex";
    private static final String REPLACEFIRST = "replace_first";
    private static final String SOURCE_FIELD = "source_field";
    private static final String FIELDS = "fields";

    private String source_field = "";
    private String fields = "";
    private String pattern = "";
    private String replacement = "";
    private Boolean isRegex = false;
    private Boolean replaceFirst = false;


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
        return CheckConfigUtil.checkAllExists(config, FIELDS, PATTERN, REPLACEMENT);
    }

    @Override
    public void prepare(FlinkEnvironment prepareEnv) {

        source_field = config.hasPath(SOURCE_FIELD) ? config.getString(SOURCE_FIELD) : "raw_message";
        fields = config.getString(FIELDS);
        pattern = config.getString(PATTERN);
        replacement = config.getString(REPLACEMENT);

        if (config.hasPath(ISREGEX)) {
            isRegex = config.getBoolean(ISREGEX);
        }
        if (config.hasPath(REPLACEFIRST)) {
            replaceFirst = config.getBoolean(REPLACEFIRST);
        }
    }

    @Override
    public void registerFunction(FlinkEnvironment flinkEnvironment) {
        FlinkStreamTransform.super.registerFunction(flinkEnvironment);

        if (flinkEnvironment.isStreaming()) {
            flinkEnvironment.getStreamTableEnvironment().registerFunction("replace", new ScalarReplace(pattern, replacement, isRegex, replaceFirst));
        } else {
            flinkEnvironment.getBatchTableEnvironment().registerFunction("replace", new ScalarReplace(pattern, replacement, isRegex, replaceFirst));
        }
    }

    @Override
    public DataSet<Row> processBatch(FlinkEnvironment env, DataSet<Row> data) {

        BatchTableEnvironment tableEnvironment = env.getBatchTableEnvironment();
        registerFunction(env);
        tableEnvironment.registerDataSet("temp", data, source_field);
        Table result = tableEnvironment.from("temp").select(source_field).as(source_field).addColumns("replace(" + source_field + ") as  " + fields);
        return TableUtil.tableToDataSet(tableEnvironment, result);
    }

    @Override
    public DataStream<Row> processStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        StreamTableEnvironment tableEnvironment = env.getStreamTableEnvironment();
        registerFunction(env);
        tableEnvironment.registerDataStream("temp", dataStream, source_field);
        Table result = tableEnvironment.from("temp").select(source_field).as(source_field).addColumns("replace(" + source_field + ") as  " + fields);
        return TableUtil.tableToDataStream(tableEnvironment, result, true);
    }
}
