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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.source.RabbitmqSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RabbitmqSplitEnumeratorTest {
    /**
     * Tests the split assignment logic of the {@link RabbitmqSplitEnumerator} in a Multi-Table
     * scenario.
     *
     * <p>This test verifies that when multiple {@link CatalogTable}s are provided during
     * initialization (representing multiple RabbitMQ queues), the enumerator successfully: 1.
     * Discovers and creates a corresponding {@link RabbitmqSplit} for each table. 2. Holds them as
     * unassigned splits initially. 3. Correctly assigns all pending splits to the reader once it is
     * registered.
     *
     * @throws Exception if an error occurs during the enumerator execution
     */
    @Test
    public void testMultiTableSplitAssignment() throws Exception {
        SourceSplitEnumerator.Context<RabbitmqSplit> context =
                mock(SourceSplitEnumerator.Context.class);

        when(context.currentParallelism()).thenReturn(1);

        Set<Integer> readers = Collections.singleton(0);
        doReturn(readers).when(context).registeredReaders();

        RabbitmqConfig config = mock(RabbitmqConfig.class);

        List<String> queues = Arrays.asList("queue_A", "queue_B");

        RabbitmqSplitEnumerator enumerator = new RabbitmqSplitEnumerator(context, config, queues);

        Assertions.assertEquals(2, enumerator.currentUnassignedSplitSize());

        enumerator.registerReader(0);

        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
        Mockito.verify(context, Mockito.times(2))
                .assignSplit(Mockito.eq(0), Mockito.any(RabbitmqSplit.class));
    }
}
