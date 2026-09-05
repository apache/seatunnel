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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.snmp.config.SnmpSinkConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.snmp4j.CommandResponderEvent;
import org.snmp4j.MessageException;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.mp.StatusInformation;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultUdpTransportMapping;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

class Snmp4jSetClientTest {

    @Test
    void testBuildsSingleBindingSetRequest() {
        SnmpSetRequest request =
                new SnmpSetRequest(new OID("1.3.6.1.2.1.1.5.0"), new Integer32(42));

        PDU pdu = Snmp4jSetClient.buildSetRequest(request);

        Assertions.assertEquals(PDU.SET, pdu.getType());
        Assertions.assertEquals(1, pdu.size());
        Assertions.assertEquals("1.3.6.1.2.1.1.5.0", pdu.get(0).getOid().toString());
        Assertions.assertEquals("42", pdu.get(0).getVariable().toString());
    }

    @Test
    void testSendsSetToLoopbackSnmpAgent() throws Exception {
        AtomicReference<VariableBinding> received = new AtomicReference<>();
        try (LoopbackAgent agent = new LoopbackAgent(PDU.noError, received);
                Snmp4jSetClient client = new Snmp4jSetClient(config(agent.getPort()))) {
            client.set(
                    new SnmpSetRequest(
                            new OID("1.3.6.1.2.1.1.5.0"),
                            SnmpSinkRowConverter.parseVariable("OctetString", "router-1")));
        }

        Assertions.assertNotNull(received.get());
        Assertions.assertEquals("1.3.6.1.2.1.1.5.0", received.get().getOid().toString());
        Assertions.assertEquals("router-1", received.get().getVariable().toString());
    }

    @Test
    void testRemoteErrorStatusIsReported() throws Exception {
        AtomicReference<VariableBinding> received = new AtomicReference<>();
        try (LoopbackAgent agent = new LoopbackAgent(PDU.notWritable, received);
                Snmp4jSetClient client = new Snmp4jSetClient(config(agent.getPort()))) {
            IOException exception =
                    Assertions.assertThrows(
                            IOException.class,
                            () ->
                                    client.set(
                                            new SnmpSetRequest(
                                                    new OID("1.3.6.1.2.1.1.5.0"),
                                                    new Integer32(42))));

            Assertions.assertTrue(
                    exception.getMessage().contains("error status " + PDU.notWritable));
            Assertions.assertTrue(exception.getMessage().contains("index 1"));
            Assertions.assertFalse(exception.getMessage().contains("unit-test-community"));
        }
    }

    private static SnmpSinkConfig config(int port) {
        Map<String, Object> values = new HashMap<>();
        values.put("host", "127.0.0.1");
        values.put("community", "unit-test-community");
        values.put("port", port);
        values.put("timeout_millis", 1000L);
        values.put("retries", 0);
        return new SnmpSinkConfig(ReadonlyConfig.fromMap(values));
    }

    private static final class LoopbackAgent implements AutoCloseable {
        private final int errorStatus;
        private final AtomicReference<VariableBinding> received;
        private final DefaultUdpTransportMapping transport;
        private final Snmp agent;
        private final AtomicReference<MessageException> responseFailure = new AtomicReference<>();

        private LoopbackAgent(int errorStatus, AtomicReference<VariableBinding> received)
                throws IOException {
            this.errorStatus = errorStatus;
            this.received = received;
            this.transport =
                    new DefaultUdpTransportMapping(
                            new UdpAddress(InetAddress.getLoopbackAddress(), 0));
            this.agent = new Snmp(transport);
            agent.addCommandResponder(this::respond);
            agent.listen();
        }

        private int getPort() {
            return transport.getListenAddress().getPort();
        }

        private void respond(CommandResponderEvent event) {
            PDU request = event.getPDU();
            received.set(request.get(0));
            PDU response = new PDU(request);
            response.setType(PDU.RESPONSE);
            response.setErrorStatus(errorStatus);
            response.setErrorIndex(errorStatus == PDU.noError ? 0 : 1);
            try {
                event.getMessageDispatcher()
                        .returnResponsePdu(
                                event.getMessageProcessingModel(),
                                event.getSecurityModel(),
                                event.getSecurityName(),
                                event.getSecurityLevel(),
                                response,
                                event.getMaxSizeResponsePDU(),
                                event.getStateReference(),
                                new StatusInformation());
                event.setProcessed(true);
            } catch (MessageException e) {
                responseFailure.set(e);
            }
        }

        @Override
        public void close() throws IOException {
            agent.close();
            if (responseFailure.get() != null) {
                throw new IOException(
                        "Loopback SNMP agent failed to send response", responseFailure.get());
            }
        }
    }
}
