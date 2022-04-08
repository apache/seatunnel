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

package org.apache.seatunnel.flink.batch;

import org.apache.seatunnel.flink.BaseFlinkTransform;
import org.apache.seatunnel.flink.FlinkEnvironment;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;

/**
 * a FlinkBatchTransform plugin will do transformations to Flink DataSet.
 */
public interface FlinkBatchTransform extends BaseFlinkTransform {

    DataSet<Row> processBatch(FlinkEnvironment env, DataSet<Row> data) throws Exception;

}
