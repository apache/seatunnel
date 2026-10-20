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
package org.apache.seatunnel.e2e.connector.snmp;

import org.snmp4j.CommandResponderEvent;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.mp.StatusInformation;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultUdpTransportMapping;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;

/** Minimal SNMP responder used to verify the sink over a real UDP transport. */
public final class SnmpAgent {

    private static final int PORT = 1161;
    private static final Path OUTPUT = Paths.get("/tmp/snmp-set.txt");

    private SnmpAgent() {}

    public static void main(String[] args) throws Exception {
        DefaultUdpTransportMapping transport =
                new DefaultUdpTransportMapping(new UdpAddress("0.0.0.0/" + PORT));
        Snmp agent = new Snmp(transport);
        agent.addCommandResponder(SnmpAgent::respond);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> close(agent)));
        agent.listen();
        System.out.println("snmp-agent-ready");
        new CountDownLatch(1).await();
    }

    private static void respond(CommandResponderEvent event) {
        PDU request = event.getPDU();
        if (request == null || request.getType() != PDU.SET || request.size() != 1) {
            return;
        }

        VariableBinding binding = request.get(0);
        try {
            Files.write(
                    OUTPUT,
                    (binding.getOid() + "=" + binding.getVariable() + System.lineSeparator())
                            .getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
            PDU response = new PDU(request);
            response.setType(PDU.RESPONSE);
            response.setErrorStatus(PDU.noError);
            response.setErrorIndex(0);
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
        } catch (IOException e) {
            throw new IllegalStateException("Failed to handle SNMP SET request", e);
        }
    }

    private static void close(Snmp agent) {
        try {
            agent.close();
        } catch (IOException e) {
            System.err.println("Failed to close SNMP agent: " + e.getMessage());
        }
    }
}
