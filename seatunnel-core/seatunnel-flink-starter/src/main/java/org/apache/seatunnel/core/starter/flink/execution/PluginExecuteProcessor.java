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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.flink.FlinkEnvironment;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.List;

public interface PluginExecuteProcessor {

    /**
     * Execute the current plugins, and return the result data stream.
     *
     * @param upstreamDataStreams the upstream data streams.
     * @return the result data stream
     */
    List<DataStream<Row>> execute(List<DataStream<Row>> upstreamDataStreams) throws TaskExecuteException;

    /**
     * Set up the flink environment.
     *
     * @param flinkEnvironment flinkEnvironment.
     */
    void setFlinkEnvironment(FlinkEnvironment flinkEnvironment);
}
