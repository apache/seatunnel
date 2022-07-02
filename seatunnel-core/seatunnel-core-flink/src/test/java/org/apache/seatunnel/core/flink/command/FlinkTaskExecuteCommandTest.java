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

package org.apache.seatunnel.core.flink.command;

import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class FlinkTaskExecuteCommandTest {

    @Test
    public void checkPluginType() {
        List<MockBatchSource> sources = Lists.newArrayList(new MockBatchSource());
        FlinkApiTaskExecuteCommand flinkTaskExecuteCommand = new FlinkApiTaskExecuteCommand(null);
        // check success
        flinkTaskExecuteCommand.checkPluginType(JobMode.BATCH, sources);
        Assert.assertThrows("checkPluginType should throw IllegalException", IllegalArgumentException.class, () -> {
            flinkTaskExecuteCommand.checkPluginType(JobMode.STREAMING, sources);
        });
    }

    private static class MockBatchSource implements FlinkBatchSource {

        @Override
        public void setConfig(Config config) {

        }

        @Override
        public Config getConfig() {
            return null;
        }

        @Override
        public DataSet<Row> getData(FlinkEnvironment env) {
            return null;
        }
    }
}
