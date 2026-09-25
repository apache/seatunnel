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

package org.apache.seatunnel.connectors.cdc.base.source.reader;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

class IncrementalSourceReaderCompatibilityTest {

    @Test
    void keepsConstructorWithoutProgressTracker() throws NoSuchMethodException {
        Assertions.assertNotNull(
                IncrementalSourceReader.class.getConstructor(
                        DataSourceDialect.class,
                        BlockingQueue.class,
                        Supplier.class,
                        RecordEmitter.class,
                        SourceReaderOptions.class,
                        SourceReader.Context.class,
                        SourceConfig.class,
                        DebeziumDeserializationSchema.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void constructorWithoutProgressTrackerAcceptsDialectWithoutName() throws Exception {
        IncrementalSourceReader<Object, SourceConfig> reader =
                new IncrementalSourceReader<>(
                        Mockito.mock(DataSourceDialect.class),
                        new ArrayBlockingQueue<>(2),
                        () -> Mockito.mock(IncrementalSourceSplitReader.class),
                        Mockito.mock(RecordEmitter.class),
                        new SourceReaderOptions(ReadonlyConfig.fromMap(Collections.emptyMap())),
                        Mockito.mock(SourceReader.Context.class),
                        Mockito.mock(SourceConfig.class),
                        Mockito.mock(DebeziumDeserializationSchema.class));
        try {
            Assertions.assertEquals("UNKNOWN", reader.getCdcProgress().getConnectorType());
        } finally {
            reader.close();
        }
    }
}
