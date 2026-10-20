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

package org.apache.seatunnel.connectors.seatunnel.snmp.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.snmp.config.SnmpSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.snmp.exception.SnmpConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class SnmpSinkWriterTest {

    @Test
    void testWritesOneSynchronousSetPerRowAndClosesClient() {
        FakeSnmpSetClient client = new FakeSnmpSetClient();
        SnmpSinkWriter writer = writer(client);

        writer.write(row());
        writer.close();

        Assertions.assertEquals(1, client.writeCount);
        Assertions.assertEquals("1.3.6.1.2.1.1.5.0", client.request.getOid().toString());
        Assertions.assertEquals("router-1", client.request.getValue().toString());
        Assertions.assertTrue(client.closed);
    }

    @Test
    void testInvalidRowDoesNotReachClient() {
        FakeSnmpSetClient client = new FakeSnmpSetClient();
        SnmpSinkWriter writer = writer(client);

        Assertions.assertThrows(
                SnmpConnectorException.class,
                () ->
                        writer.write(
                                new SeaTunnelRow(
                                        new Object[] {"invalid", "router-1", "OctetString"})));

        Assertions.assertEquals(0, client.writeCount);
    }

    @Test
    void testWriteFailureUsesConnectorErrorWithoutCommunity() {
        FakeSnmpSetClient client = new FakeSnmpSetClient();
        client.writeFailure = new IOException("remote timeout");
        SnmpSinkWriter writer = writer(client);

        SnmpConnectorException exception =
                Assertions.assertThrows(SnmpConnectorException.class, () -> writer.write(row()));

        Assertions.assertTrue(exception.getMessage().contains("SNMP-03"));
        Assertions.assertTrue(exception.getMessage().contains("1.3.6.1.2.1.1.5.0"));
        Assertions.assertFalse(exception.getMessage().contains("unit-test-community"));
    }

    @Test
    void testConnectionAndCloseFailuresUseConnectorErrorsWithoutCommunity() {
        SnmpSinkConfig config = SnmpSinkRowConverterTest.config();
        SnmpConnectorException connectionFailure =
                Assertions.assertThrows(
                        SnmpConnectorException.class,
                        () ->
                                new SnmpSinkWriter(
                                        config,
                                        SnmpSinkRowConverterTest.sinkRowType(),
                                        ignored -> {
                                            throw new IOException("bind failed");
                                        }));
        Assertions.assertTrue(connectionFailure.getMessage().contains("SNMP-01"));
        Assertions.assertFalse(connectionFailure.getMessage().contains("unit-test-community"));

        FakeSnmpSetClient client = new FakeSnmpSetClient();
        client.closeFailure = new IOException("close failed");
        SnmpConnectorException closeFailure =
                Assertions.assertThrows(SnmpConnectorException.class, () -> writer(client).close());
        Assertions.assertTrue(closeFailure.getMessage().contains("SNMP-06"));
        Assertions.assertFalse(closeFailure.getMessage().contains("unit-test-community"));
    }

    private static SnmpSinkWriter writer(FakeSnmpSetClient client) {
        return new SnmpSinkWriter(
                SnmpSinkRowConverterTest.config(),
                SnmpSinkRowConverterTest.sinkRowType(),
                ignored -> client);
    }

    private static SeaTunnelRow row() {
        return new SeaTunnelRow(new Object[] {"1.3.6.1.2.1.1.5.0", "router-1", "OctetString"});
    }

    private static final class FakeSnmpSetClient implements SnmpSetClient {
        private int writeCount;
        private boolean closed;
        private SnmpSetRequest request;
        private IOException writeFailure;
        private IOException closeFailure;

        @Override
        public void set(SnmpSetRequest request) throws IOException {
            writeCount++;
            this.request = request;
            if (writeFailure != null) {
                throw writeFailure;
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }
}
