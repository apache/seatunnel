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

package org.apache.seatunnel.connectors.seatunnel.iotdb.sink;

import org.apache.seatunnel.connectors.seatunnel.iotdb.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iotdb.exception.IotdbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iotdb.serialize.IoTDBRecord;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IoTDBSinkClientTest {

    private static final String REDIRECTION = "400: ";
    private static final String OTHER_ERROR = "500: something is wrong with the data";

    /** Stub node: only accepts the devices it is the leader of, others get a 400 redirect. */
    static class StubNode extends Session {
        private final Set<String> leaderOf;
        final List<String> acceptedDevices = new ArrayList<>();
        int attempts;

        StubNode(Set<String> leaderOf) {
            super("127.0.0.1", 6667, "root", "root");
            this.leaderOf = leaderOf;
        }

        @Override
        public void close() {}

        @Override
        public void insertRecords(
                List<String> deviceIds,
                List<Long> times,
                List<List<String>> measurementsList,
                List<List<TSDataType>> typesList,
                List<List<Object>> valuesList)
                throws IoTDBConnectionException, StatementExecutionException {
            attempts++;
            for (String device : deviceIds) {
                if (!leaderOf.contains(device)) {
                    throw new StatementExecutionException(REDIRECTION);
                }
            }
            acceptedDevices.addAll(deviceIds);
        }
    }

    private static SinkConfig config() {
        SinkConfig config =
                new SinkConfig(Arrays.asList("n0:6667", "n1:6667", "n2:6667"), "root", "root");
        config.setBatchSize(100);
        config.setMaxRetries(1);
        config.setRetryBackoffMultiplierMs(1);
        config.setMaxRetryBackoffMs(1);
        return config;
    }

    private static IoTDBRecord record(String device, long timestamp) {
        return new IoTDBRecord(
                device,
                timestamp,
                Arrays.asList("value"),
                Arrays.asList(TSDataType.DOUBLE),
                Arrays.asList(1.0d));
    }

    private static IoTDBSinkClient clientRoutingTo(final List<StubNode> nodes) {
        return new IoTDBSinkClient(config()) {
            @Override
            protected Session buildSession(String host, int port) {
                return nodes.get(Integer.parseInt(host.substring(1)));
            }
        };
    }

    @Test
    public void shouldRouteEachDeviceToItsLeaderAndCacheIt() throws Exception {
        // node0 leads nothing, node1 leads d_a, node2 leads d_b
        StubNode n0 = new StubNode(new HashSet<>());
        StubNode n1 = new StubNode(new HashSet<>(Arrays.asList("sg.d_a")));
        StubNode n2 = new StubNode(new HashSet<>(Arrays.asList("sg.d_b")));
        IoTDBSinkClient client = clientRoutingTo(Arrays.asList(n0, n1, n2));

        for (int i = 0; i < 10; i++) {
            client.write(record("sg.d_a", i));
            client.write(record("sg.d_b", i));
        }
        client.close();

        Assertions.assertEquals(10, count(n1.acceptedDevices, "sg.d_a"));
        Assertions.assertEquals(10, count(n2.acceptedDevices, "sg.d_b"));

        // second flush goes straight to the leaders: node0 must not be contacted again
        int n0AttemptsBefore = n0.attempts;
        for (int i = 10; i < 15; i++) {
            client.write(record("sg.d_a", i));
            client.write(record("sg.d_b", i));
        }
        client.close();
        Assertions.assertEquals(n0AttemptsBefore, n0.attempts);
        Assertions.assertEquals(15, count(n1.acceptedDevices, "sg.d_a"));
        Assertions.assertEquals(15, count(n2.acceptedDevices, "sg.d_b"));
    }

    @Test
    public void shouldFailFastOnDataLevelRejection() throws Exception {
        StubNode n0 =
                new StubNode(new HashSet<>()) {
                    @Override
                    public void insertRecords(
                            List<String> deviceIds,
                            List<Long> times,
                            List<List<String>> measurementsList,
                            List<List<TSDataType>> typesList,
                            List<List<Object>> valuesList)
                            throws StatementExecutionException {
                        attempts++;
                        throw new StatementExecutionException(OTHER_ERROR);
                    }
                };
        IoTDBSinkClient client =
                clientRoutingTo(
                        Arrays.asList(
                                n0, new StubNode(new HashSet<>()), new StubNode(new HashSet<>())));

        IotdbConnectorException error =
                Assertions.assertThrows(
                        IotdbConnectorException.class,
                        () -> {
                            client.write(record("sg.d_a", 1L));
                            client.close();
                        });
        Assertions.assertTrue(error.getMessage().contains("sg.d_a"));
        Assertions.assertEquals(1, n0.attempts);
    }

    @Test
    public void shouldBackOffAndRetryOnNetworkFailureThenGiveUp() throws Exception {
        StubNode n0 =
                new StubNode(new HashSet<>()) {
                    @Override
                    public void insertRecords(
                            List<String> deviceIds,
                            List<Long> times,
                            List<List<String>> measurementsList,
                            List<List<TSDataType>> typesList,
                            List<List<Object>> valuesList)
                            throws IoTDBConnectionException {
                        attempts++;
                        throw new IoTDBConnectionException("connection refused");
                    }
                };
        IoTDBSinkClient client =
                clientRoutingTo(
                        Arrays.asList(
                                n0, new StubNode(new HashSet<>()), new StubNode(new HashSet<>())));

        Assertions.assertThrows(
                IotdbConnectorException.class,
                () -> {
                    client.write(record("sg.d_a", 1L));
                    client.close();
                });
        // node count (3) + retry budget (1) + 1 rounds were attempted before giving up
        Assertions.assertEquals(5, n0.attempts);
    }

    private static int count(List<String> devices, String device) {
        return (int) devices.stream().filter(device::equals).count();
    }
}
