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

import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SIZE;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SIZE_DEFAULT_VALUE;

import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.flink.BaseFlinkSource;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

@AutoService(BaseFlinkSource.class)
public class FakeSource implements FlinkBatchSource {

    private Config config;

    private List<MockSchema> mockDataSchema;
    private int mockDataSize;

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
        mockDataSize = TypesafeConfigUtils.getConfig(config, MOCK_DATA_SIZE, MOCK_DATA_SIZE_DEFAULT_VALUE);
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
        List<Row> dataSet = new ArrayList<>(mockDataSize);
        for (long index = 0; index < mockDataSize; index++) {
            dataSet.add(MockSchema.mockRowData(mockDataSchema));
        }
        return env.getBatchEnvironment()
            .fromCollection(
                dataSet,
                MockSchema.mockRowTypeInfo(mockDataSchema)
            );
    }

}
