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

import org.apache.seatunnel.api.sink.SinkWriter;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nullable;

public class SparkDataWriterFactory implements DataWriterFactory<InternalRow> {

    @Nullable
    private final SinkWriter.Context context;
    private final StructType schema;
    private final String sinkString;

    SparkDataWriterFactory(@Nullable SinkWriter.Context context,
                           StructType schema, String sinkString) {
        this.context = context;
        this.schema = schema;
        this.sinkString = sinkString;
    }

    @Override
    public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
        // TODO use partitionID, taskId information.
        return new SparkDataWriter<>(context, schema, epochId, sinkString);
    }
}
