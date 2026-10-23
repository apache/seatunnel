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

package org.apache.seatunnel.connectors.seatunnel.snmp.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.snmp.config.SnmpSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.snmp.exception.SnmpConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.snmp4j.smi.OID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

class SnmpSourceTest {

    @Test
    void testSourceMetadataAndBoundedness() {
        SnmpSource source = new SnmpSource(config());

        Assertions.assertEquals("SNMP", source.getPluginName());
        Assertions.assertEquals(Boundedness.BOUNDED, source.getBoundedness());

        source.setJobContext(new JobContext().setJobMode(JobMode.STREAMING));
        Assertions.assertEquals(Boundedness.UNBOUNDED, source.getBoundedness());
    }

    @Test
    void testFixedOutputSchema() {
        CatalogTable table = SnmpSource.createCatalogTable();

        Assertions.assertArrayEquals(
                new String[] {"agent", "oid", "value", "value_type", "poll_time"},
                table.getSeaTunnelRowType().getFieldNames());
        Assertions.assertArrayEquals(
                new Object[] {
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.LONG_TYPE
                },
                table.getSeaTunnelRowType().getFieldTypes());
    }

    @Test
    void testSourceSerializationForWorkerExecution() {
        SnmpSource source = new SnmpSource(config());

        SnmpSource restored = SerializationUtils.deserialize(SerializationUtils.serialize(source));

        Assertions.assertEquals("SNMP", restored.getPluginName());
        Assertions.assertArrayEquals(
                source.getProducedCatalogTables().get(0).getSeaTunnelRowType().getFieldNames(),
                restored.getProducedCatalogTables().get(0).getSeaTunnelRowType().getFieldNames());
    }

    @Test
    void testBoundedReaderPollsOnceAndSignalsCompletion() throws Exception {
        TestReaderContext testContext = new TestReaderContext(Boundedness.BOUNDED);
        SingleSplitReaderContext context = new SingleSplitReaderContext(testContext);
        FakeSnmpClient client = new FakeSnmpClient();
        SnmpSourceReader reader = reader(context, client, new AtomicLong(1234L));
        RecordingCollector collector = new RecordingCollector();
        reader.open();

        reader.pollNext(collector);
        reader.pollNext(collector);

        Assertions.assertEquals(1, client.pollCount);
        Assertions.assertEquals(2, collector.rows.size());
        Assertions.assertArrayEquals(
                new Object[] {"127.0.0.1:161", "1.3.6.1.2.1.1.3.0", "42", "TimeTicks", 1234L},
                collector.rows.get(0).getFields());
        Assertions.assertTrue(testContext.noMoreElements);
    }

    @Test
    void testStreamingReaderHonorsPollInterval() throws Exception {
        TestReaderContext testContext = new TestReaderContext(Boundedness.UNBOUNDED);
        SingleSplitReaderContext context = new SingleSplitReaderContext(testContext);
        FakeSnmpClient client = new FakeSnmpClient();
        AtomicLong clock = new AtomicLong(1000L);
        SnmpSourceReader reader = reader(context, client, clock);
        RecordingCollector collector = new RecordingCollector();
        reader.open();

        reader.pollNext(collector);
        clock.set(1099L);
        reader.pollNext(collector);
        Assertions.assertEquals(1, client.pollCount);

        clock.set(1100L);
        reader.pollNext(collector);
        Assertions.assertEquals(2, client.pollCount);
        Assertions.assertFalse(testContext.noMoreElements);
    }

    @Test
    void testCloseStopsPollingAndClosesClient() throws Exception {
        SingleSplitReaderContext context =
                new SingleSplitReaderContext(new TestReaderContext(Boundedness.UNBOUNDED));
        FakeSnmpClient client = new FakeSnmpClient();
        SnmpSourceReader reader = reader(context, client, new AtomicLong(1000L));
        reader.open();

        reader.close();
        reader.pollNext(new RecordingCollector());

        Assertions.assertTrue(client.closed);
        Assertions.assertEquals(0, client.pollCount);
    }

    @Test
    void testPollFailureDoesNotDiscloseCommunity() throws Exception {
        SingleSplitReaderContext context =
                new SingleSplitReaderContext(new TestReaderContext(Boundedness.BOUNDED));
        FakeSnmpClient client = new FakeSnmpClient();
        client.failure = new IOException("request failed");
        SnmpSourceReader reader = reader(context, client, new AtomicLong(1000L));
        reader.open();

        SnmpConnectorException exception =
                Assertions.assertThrows(
                        SnmpConnectorException.class,
                        () -> reader.pollNext(new RecordingCollector()));

        Assertions.assertFalse(exception.getMessage().contains("unit-test-community"));
    }

    private static SnmpSourceReader reader(
            SingleSplitReaderContext context, FakeSnmpClient client, AtomicLong clock) {
        return new SnmpSourceReader(config(), context, ignored -> client, clock::get);
    }

    private static SnmpSourceConfig config() {
        Map<String, Object> values = new HashMap<>();
        values.put("host", "127.0.0.1");
        values.put("community", "unit-test-community");
        values.put("oids", Arrays.asList("1.3.6.1.2.1.1.3.0", "1.3.6.1.2.1.1.5.0"));
        values.put("poll_interval_millis", 100L);
        return new SnmpSourceConfig(ReadonlyConfig.fromMap(values));
    }

    private static class FakeSnmpClient implements SnmpClient {
        private int pollCount;
        private boolean closed;
        private IOException failure;

        @Override
        public List<SnmpRecord> get(List<OID> oids) throws IOException {
            pollCount++;
            if (failure != null) {
                throw failure;
            }
            return Arrays.asList(
                    new SnmpRecord(oids.get(0).toString(), "42", "TimeTicks"),
                    new SnmpRecord(oids.get(1).toString(), "router-1", "OctetString"));
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static class RecordingCollector implements Collector<SeaTunnelRow> {
        private final Object checkpointLock = new Object();
        private final List<SeaTunnelRow> rows = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow record) {
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }
    }

    private static class TestReaderContext implements SourceReader.Context {
        private final Boundedness boundedness;
        private boolean noMoreElements;

        private TestReaderContext(Boundedness boundedness) {
            this.boundedness = boundedness;
        }

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public Boundedness getBoundedness() {
            return boundedness;
        }

        @Override
        public void signalNoMoreElement() {
            noMoreElements = true;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {}

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }
}
