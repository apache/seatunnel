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

package org.apache.seatunnel.translation.spark.sink;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;

import org.apache.spark.sql.types.StructType;

import javax.annotation.Nullable;

public abstract class AbstractSparkWriterConverter {

    protected final SinkWriter.Context context;
    protected final SinkCommitter<?> sinkCommitter;
    protected final SinkAggregatedCommitter<?, ?> sinkAggregatedCommitter;
    protected final StructType schema;
    protected final String sinkString;

    AbstractSparkWriterConverter(SinkWriter.Context context, SinkCommitter<?> sinkCommitter,
                                 @Nullable SinkAggregatedCommitter<?, ?> sinkAggregatedCommitter,
                                 StructType schema, String sinkString) {
        this.context = context;
        this.sinkCommitter = sinkCommitter;
        this.sinkAggregatedCommitter = sinkAggregatedCommitter;
        this.schema = schema;
        this.sinkString = sinkString;
    }
}
