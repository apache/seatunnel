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

package org.apache.seatunnel.connectors.seatunnel.common.source.arrow.reader;

import org.apache.seatunnel.shade.org.apache.arrow.memory.RootAllocator;
import org.apache.seatunnel.shade.org.apache.arrow.vector.ipc.ArrowStreamReader;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;

/**
 * Verifies that Arrow reader cleanup releases the allocator even when stream reader close fails.
 */
public class ArrowToSeatunnelRowReaderCloseTest {

    /**
     * Covers the normal close path and preserves the required reader-before-allocator order.
     *
     * <p>This protects the Arrow allocator from being closed before the stream reader has released
     * vector buffers.
     */
    @Test
    public void testCloseReleasesAllocatorWhenStreamReaderCloseSucceeds() throws Exception {
        ArrowStreamReader arrowStreamReader = Mockito.mock(ArrowStreamReader.class);
        RootAllocator rootAllocator = Mockito.mock(RootAllocator.class);
        ArrowToSeatunnelRowReader reader =
                new ArrowToSeatunnelRowReader(
                        arrowStreamReader, rootAllocator, createSeatunnelRowType());

        reader.close();

        InOrder inOrder = Mockito.inOrder(arrowStreamReader, rootAllocator);
        inOrder.verify(arrowStreamReader).close();
        inOrder.verify(rootAllocator).close();
    }

    /**
     * Covers the regression where an ArrowStreamReader close failure skipped allocator cleanup.
     *
     * <p>The reader close failure is still propagated, but the allocator must always receive its
     * close call.
     */
    @Test
    public void testCloseReleasesAllocatorWhenStreamReaderCloseFails() throws Exception {
        IOException closeFailure = new IOException("close failure");
        ArrowStreamReader arrowStreamReader = Mockito.mock(ArrowStreamReader.class);
        Mockito.doThrow(closeFailure).when(arrowStreamReader).close();
        RootAllocator rootAllocator = Mockito.mock(RootAllocator.class);
        ArrowToSeatunnelRowReader reader =
                new ArrowToSeatunnelRowReader(
                        arrowStreamReader, rootAllocator, createSeatunnelRowType());

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, reader::close);

        Assertions.assertSame(closeFailure, exception.getCause());
        InOrder inOrder = Mockito.inOrder(arrowStreamReader, rootAllocator);
        inOrder.verify(arrowStreamReader).close();
        inOrder.verify(rootAllocator).close();
    }

    /**
     * Builds the minimal SeaTunnel schema needed to initialize the reader resource owner.
     *
     * <p>The close tests only need a valid schema for constructor initialization.
     */
    private SeaTunnelRowType createSeatunnelRowType() {
        return new SeaTunnelRowType(
                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
    }
}
