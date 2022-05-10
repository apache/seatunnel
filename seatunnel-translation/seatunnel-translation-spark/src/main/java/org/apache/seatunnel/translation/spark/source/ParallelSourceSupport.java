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

package org.apache.seatunnel.translation.spark.source;

import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.ContinuousReadSupport;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

public class ParallelSourceSupport implements DataSourceV2, ReadSupport, MicroBatchReadSupport, ContinuousReadSupport, DataSourceRegister {

    public static final String SEA_TUNNEL_SOURCE_NAME = "SeaTunnel";

    @Override
    public String shortName() {
        return SEA_TUNNEL_SOURCE_NAME;
    }

    @Override
    public DataSourceReader createReader(StructType schema, DataSourceOptions options) {
        return ReadSupport.super.createReader(schema, options);
    }

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        return null;
    }

    @Override
    public MicroBatchReader createMicroBatchReader(Optional<StructType> schema, String checkpointLocation, DataSourceOptions options) {
        return null;
    }

    @Override
    public ContinuousReader createContinuousReader(Optional<StructType> schema, String checkpointLocation, DataSourceOptions options) {
        return null;
    }
}
