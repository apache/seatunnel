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

package org.apache.seatunnel.connectors.seatunnel.qdrant.sink;

import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.qdrant.client.grpc.JsonWithInt;

/**
 * Verifies that payload values keep the shape their SeaTunnel type declares.
 *
 * <p>The sink documents {@code BIGINT -> payload integer} and {@code FLOAT/DOUBLE -> payload
 * double}, so a numeric payload must be stored with the matching Qdrant value type and without
 * losing range or precision.
 */
public class QdrantBatchWriterTest {

    @Test
    public void testDoublePayloadKeepsFractionalPart() {
        JsonWithInt.Value value = QdrantBatchWriter.buildPayload(BasicType.DOUBLE_TYPE, 1.5d);

        Assertions.assertEquals(1.5d, value.getDoubleValue(), 0.0d);
    }

    @Test
    public void testFloatPayloadKeepsFractionalPart() {
        JsonWithInt.Value value = QdrantBatchWriter.buildPayload(BasicType.FLOAT_TYPE, 2.5f);

        Assertions.assertEquals(2.5d, value.getDoubleValue(), 0.0d);
    }

    @Test
    public void testBigintPayloadKeepsFullRange() {
        JsonWithInt.Value value =
                QdrantBatchWriter.buildPayload(BasicType.LONG_TYPE, 5_000_000_000L);

        Assertions.assertEquals(5_000_000_000L, value.getIntegerValue());
    }

    @Test
    public void testIntPayloadStaysInteger() {
        JsonWithInt.Value value = QdrantBatchWriter.buildPayload(BasicType.INT_TYPE, 42);

        Assertions.assertEquals(42L, value.getIntegerValue());
    }
}
