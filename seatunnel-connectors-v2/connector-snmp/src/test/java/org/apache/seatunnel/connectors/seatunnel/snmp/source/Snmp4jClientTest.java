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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.snmp.config.SnmpSourceConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.snmp4j.CommandResponderEvent;
import org.snmp4j.CommunityTarget;
import org.snmp4j.MessageException;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.mp.StatusInformation;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultUdpTransportMapping;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

class Snmp4jClientTest {

    @Test
    void testBuildsSnmpV2cGetRequestAndTarget() {
        SnmpSourceConfig config = config();

        PDU request = Snmp4jClient.buildGetRequest(config.getOids());
        Target target = Snmp4jClient.buildTarget(config);

        Assertions.assertEquals(PDU.GET, request.getType());
        Assertions.assertEquals(2, request.size());
        Assertions.assertEquals("1.3.6.1.2.1.1.3.0", request.get(0).getOid().toString());
        Assertions.assertEquals(SnmpConstants.version2c, target.getVersion());
        Assertions.assertEquals(2500L, target.getTimeout());
        Assertions.assertEquals(2, target.getRetries());
        Assertions.assertEquals("127.0.0.1/1161", target.getAddress().toString());
        Assertions.assertEquals(
                "unit-test-community", ((CommunityTarget) target).getCommunity().toString());
    }

    @Test
    void testExtractsStableRecordFields() {
        PDU response = new PDU();
        response.add(new VariableBinding(new OID("1.3.6.1.2.1.1.3.0"), new Integer32(42)));

        List<SnmpRecord> records = Snmp4jClient.extractRecords(response);

        Assertions.assertEquals(1, records.size());
        Assertions.assertEquals("1.3.6.1.2.1.1.3.0", records.get(0).getOid());
        Assertions.assertEquals("42", records.get(0).getValue());
        Assertions.assertEquals("Integer32", records.get(0).getValueType());
    }

    @Test
    void testClosesTransportWhenListenFails() throws Exception {
        FailingSnmp snmp = new FailingSnmp();

        IOException exception =
                Assertions.assertThrows(IOException.class, () -> new Snmp4jClient(config(), snmp));

        Assertions.assertEquals("listen failed", exception.getMessage());
        Assertions.assertTrue(snmp.closed);
    }

    @Test
    void testPollsLocalSnmpV2cAgent() throws Exception {
        DefaultUdpTransportMapping transport =
                new DefaultUdpTransportMapping(new UdpAddress(InetAddress.getLoopbackAddress(), 0));
        AtomicReference<MessageException> responseFailure = new AtomicReference<>();
        Snmp agent = new Snmp(transport);
        try {
            agent.addCommandResponder(event -> respond(event, responseFailure));
            agent.listen();

            Map<String, Object> values = baseConfig();
            values.put("port", transport.getListenAddress().getPort());
            try (Snmp4jClient client =
                    new Snmp4jClient(new SnmpSourceConfig(ReadonlyConfig.fromMap(values)))) {
                List<SnmpRecord> records =
                        client.get(
                                Arrays.asList(
                                        new OID("1.3.6.1.2.1.1.3.0"),
                                        new OID("1.3.6.1.2.1.1.5.0")));

                Assertions.assertEquals(2, records.size());
                Assertions.assertEquals("123", records.get(0).getValue());
                Assertions.assertEquals("Integer32", records.get(0).getValueType());
                Assertions.assertNull(responseFailure.get());
            }
        } finally {
            agent.close();
        }
    }

    private static void respond(
            CommandResponderEvent event, AtomicReference<MessageException> responseFailure) {
        PDU response = new PDU(event.getPDU());
        response.setType(PDU.RESPONSE);
        for (int index = 0; index < response.size(); index++) {
            response.get(index).setVariable(new Integer32(123 + index));
        }
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

    private static SnmpSourceConfig config() {
        Map<String, Object> values = baseConfig();
        values.put("timeout_millis", 2500L);
        values.put("retries", 2);
        return new SnmpSourceConfig(ReadonlyConfig.fromMap(values));
    }

    private static Map<String, Object> baseConfig() {
        Map<String, Object> values = new HashMap<>();
        values.put("host", "127.0.0.1");
        values.put("port", 1161);
        values.put("community", "unit-test-community");
        values.put("oids", Arrays.asList("1.3.6.1.2.1.1.3.0", "1.3.6.1.2.1.1.5.0"));
        return values;
    }

    private static class FailingSnmp extends Snmp {
        private boolean closed;

        @Override
        public void listen() throws IOException {
            throw new IOException("listen failed");
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
