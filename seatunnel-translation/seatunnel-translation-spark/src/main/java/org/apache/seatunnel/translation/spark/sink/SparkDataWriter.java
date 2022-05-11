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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.spark.serialization.SparkRowSerialization;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import java.io.IOException;

public class SparkDataWriter<CommitInfoT, StateT> implements DataWriter<InternalRow> {

    private final SinkWriter<SeaTunnelRow, CommitInfoT, StateT> sinkWriter;
    private final SparkRowSerialization rowSerialization = new SparkRowSerialization();

    SparkDataWriter(SinkWriter<SeaTunnelRow, CommitInfoT, StateT> sinkWriter) {
        this.sinkWriter = sinkWriter;
    }

    @Override
    public void write(InternalRow record) throws IOException {
        sinkWriter.write(rowSerialization.deserialize(record));
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        sinkWriter.prepareCommit();
    }

    @Override
    public void abort() throws IOException {

    }
}
