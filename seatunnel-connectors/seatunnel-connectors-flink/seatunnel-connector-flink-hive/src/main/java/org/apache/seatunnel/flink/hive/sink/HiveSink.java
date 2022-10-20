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

package org.apache.seatunnel.flink.hive.sink;

import static org.apache.seatunnel.flink.hive.config.HiveConfig.SQL;

import org.apache.seatunnel.flink.BaseFlinkSink;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.hive.common.BaseHivePlugin;
import org.apache.seatunnel.flink.stream.FlinkStreamSink;

import com.google.auto.service.AutoService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

@AutoService(BaseFlinkSink.class)
public class HiveSink extends BaseHivePlugin implements FlinkStreamSink {
    @Override
    public void outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        StreamTableEnvironment streamTableEnvironment = env.getStreamTableEnvironment();
        streamTableEnvironment.executeSql(config.getString(SQL));
    }
}
