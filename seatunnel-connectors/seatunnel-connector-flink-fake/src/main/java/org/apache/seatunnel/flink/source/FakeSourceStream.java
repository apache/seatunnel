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

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.types.Row;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.stream.FlinkStreamSource;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;

public class FakeSourceStream extends RichParallelSourceFunction<Row> implements FlinkStreamSource<Row> {

    private volatile boolean running = true;

    private Config config;

    @Override
    public DataStream<Row> getData(FlinkEnvironment env) {
        return env.getStreamExecutionEnvironment()
                .addSource(this)
                .returns(new RowTypeInfo(STRING_TYPE_INFO, LONG_TYPE_INFO));
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
        return new CheckResult(true, "");
    }

    @Override
    public void prepare(FlinkEnvironment env) {
    }

    private static final String[] NAME_ARRAY = new String[]{"Gary", "Ricky Huo", "Kid Xiong"};

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        while (running) {
            int randomNum = (int) (1 + Math.random() * 3);
            Row row = Row.of(NAME_ARRAY[randomNum - 1], System.currentTimeMillis());
            ctx.collect(row);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
