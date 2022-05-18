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

import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_INTERVAL;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_INTERVAL_DEFAULT_VALUE;

import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.flink.BaseFlinkSource;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.stream.FlinkStreamSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.concurrent.TimeUnit;

@AutoService(BaseFlinkSource.class)
public class FakeSourceStream extends RichParallelSourceFunction<Row> implements FlinkStreamSource {

    private static final long serialVersionUID = -3026082767246767679L;
    private volatile boolean running = true;
    private static final String PARALLELISM = "parallelism";

    private Config config;

    private List<MockSchema> mockDataSchema;
    private long mockDataInterval;

    @Override
    public DataStream<Row> getData(FlinkEnvironment env) {
        DataStreamSource<Row> source = env.getStreamExecutionEnvironment().addSource(this);
        if (config.hasPath(PARALLELISM)) {
            source = source.setParallelism(config.getInt(PARALLELISM));
        }
        return source.returns(MockSchema.mockRowTypeInfo(mockDataSchema));
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
    public void prepare(FlinkEnvironment env) {
        mockDataSchema = MockSchema.resolveConfig(config);
        mockDataInterval = TypesafeConfigUtils.getConfig(config, MOCK_DATA_INTERVAL, MOCK_DATA_INTERVAL_DEFAULT_VALUE);
    }

    @Override
    public String getPluginName() {
        return "FakeSourceStream";
    }

    @Override
    public void run(SourceFunction.SourceContext<Row> ctx) throws Exception {
        while (running){
            Row rowData = MockSchema.mockRowData(mockDataSchema);
            ctx.collect(rowData);
            Thread.sleep(TimeUnit.SECONDS.toMillis(mockDataInterval));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
