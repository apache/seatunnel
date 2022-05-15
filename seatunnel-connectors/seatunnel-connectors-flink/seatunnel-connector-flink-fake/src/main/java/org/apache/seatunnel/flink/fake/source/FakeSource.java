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

import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_ENABLE;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_MOCK;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_NAME;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SCHEMA_TYPE;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SIZE;
import static org.apache.seatunnel.flink.fake.Config.MOCK_DATA_SIZE_DEFAULT_VALUE;

import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.BaseFlinkSource;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

@AutoService(BaseFlinkSource.class)
public class FakeSource implements FlinkBatchSource {

    private static final String[] NAME_ARRAY = new String[]{"Gary", "Ricky Huo", "Kid Xiong"};
    private Config config;
    private static final int AGE_LIMIT = 100;

    private List<MockSchema> mockDataSchema;
    private boolean mockDataEnable;
    private long mockDataSize;

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
        mockDataEnable = config.hasPath(MOCK_DATA_ENABLE) && config.getBoolean(MOCK_DATA_ENABLE);
        if (mockDataEnable) {
            if (config.hasPath(MOCK_DATA_SCHEMA)) {
                mockDataSchema = config.getConfigList(MOCK_DATA_SCHEMA)
                    .stream()
                    .map(
                        schemaConfig -> {
                            MockSchema schema = new MockSchema();
                            schema.setName(schemaConfig.getString(MOCK_DATA_SCHEMA_NAME));
                            schema.setType(schemaConfig.getString(MOCK_DATA_SCHEMA_TYPE));
                            if (schemaConfig.hasPath(MOCK_DATA_SCHEMA_MOCK)) {
                                schema.setMockConfig(schemaConfig.getConfig(MOCK_DATA_SCHEMA_MOCK));
                            }
                            return schema;
                        }
                    )
                    .collect(Collectors.toList());
            } else {
                mockDataSchema = new ArrayList<>(0);
                for (String name : NAME_ARRAY) {
                    MockSchema schema = new MockSchema();
                    schema.setName(name);
                    schema.setType("string");
                    mockDataSchema.add(schema);
                }
            }
            mockDataSize =
                config.hasPath(MOCK_DATA_SIZE) ?
                    config.getLong(MOCK_DATA_SIZE) :
                    MOCK_DATA_SIZE_DEFAULT_VALUE;
        }
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
        if (this.mockDataEnable) {
            List<Row> dataSet = new ArrayList<>(0);
            for (long index = 0; index < mockDataSize; index++) {
                dataSet.add(MockSchema.mockRowData(mockDataSchema));
            }
            return env.getBatchEnvironment()
                .fromCollection(
                    dataSet,
                    MockSchema.mockRowTypeInfo(mockDataSchema)
                );
        } else {
            Random random = new Random();
            return env.getBatchTableEnvironment().toDataSet(
                env.getBatchTableEnvironment().fromValues(
                    DataTypes.ROW(DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("age", DataTypes.INT())),
                    Arrays.stream(NAME_ARRAY).map(n -> Row.of(n, random.nextInt(AGE_LIMIT)))
                        .collect(Collectors.toList())), Row.class);
        }
    }

}
