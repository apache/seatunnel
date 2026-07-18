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

package org.apache.seatunnel.engine.server.telemetry.metrics.exports;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.jmx.PartitionServiceMBean;
import com.hazelcast.logging.ILogger;

import java.util.ArrayList;

class NodeMetricExportsTest {

    @Test
    void partitionMetricsShouldBeSkippedWhenHazelcastIsShuttingDown() {
        Node node = Mockito.mock(Node.class);
        ILogger logger = Mockito.mock(ILogger.class);
        Mockito.when(node.getLogger(NodeMetricExports.class)).thenReturn(logger);
        PartitionServiceMBean partitionServiceMBean = Mockito.mock(PartitionServiceMBean.class);
        Mockito.when(partitionServiceMBean.getPartitionCount())
                .thenThrow(new HazelcastInstanceNotActiveException("SHUT_DOWN"));
        NodeMetricExports exports = new NodeMetricExports(node);
        ArrayList<io.prometheus.client.Collector.MetricFamilySamples> metrics = new ArrayList<>();

        Assertions.assertDoesNotThrow(
                () -> exports.partitionMetric(partitionServiceMBean, metrics, "127.0.0.1:5801"));
        Assertions.assertTrue(metrics.isEmpty());
    }
}
