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

package org.apache.seatunnel.core.starter.spark.execution;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.spark.sink.SeaTunnelBatchWrite;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class Spark35BatchWriteTest {

    @Test
    public void useCommitCoordinator() throws IOException {
        SeaTunnelBatchWrite<Void, Void, Void> batchWrite =
                new SeaTunnelBatchWrite<>(new TestSink(), new CatalogTable[0], "test", 1);

        Assertions.assertTrue(batchWrite.useCommitCoordinator());
    }

    private static class TestSink implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void> {

        @Override
        public String getPluginName() {
            return "TestSink";
        }

        @Override
        public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context) {
            throw new UnsupportedOperationException();
        }
    }
}
