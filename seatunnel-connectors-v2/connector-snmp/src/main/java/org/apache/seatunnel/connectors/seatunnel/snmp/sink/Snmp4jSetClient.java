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

import org.apache.seatunnel.connectors.seatunnel.snmp.client.SnmpTargetFactory;
import org.apache.seatunnel.connectors.seatunnel.snmp.config.SnmpSinkConfig;

import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultUdpTransportMapping;

import java.io.IOException;

/** SNMPv2c SET client backed by SNMP4J. */
final class Snmp4jSetClient implements SnmpSetClient {

    private final SnmpSinkConfig config;
    private final Snmp snmp;
    private final Target target;

    Snmp4jSetClient(SnmpSinkConfig config) throws IOException {
        this(config, new Snmp(new DefaultUdpTransportMapping()));
    }

    Snmp4jSetClient(SnmpSinkConfig config, Snmp snmp) throws IOException {
        this.config = config;
        Target createdTarget;
        try {
            createdTarget = SnmpTargetFactory.create(config);
            snmp.listen();
        } catch (IOException | RuntimeException e) {
            try {
                snmp.close();
            } catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
        this.target = createdTarget;
        this.snmp = snmp;
    }

    @Override
    public void set(SnmpSetRequest request) throws IOException {
        ResponseEvent event = snmp.send(buildSetRequest(request), target);
        if (event == null || event.getResponse() == null) {
            throw new IOException(
                    "SNMP SET request timed out for agent "
                            + config.getHost()
                            + ":"
                            + config.getPort());
        }

        PDU response = event.getResponse();
        if (response.getErrorStatus() != PDU.noError) {
            throw new IOException(
                    "SNMP agent returned error status "
                            + response.getErrorStatus()
                            + " ("
                            + response.getErrorStatusText()
                            + ") at index "
                            + response.getErrorIndex());
        }
    }

    @Override
    public void close() throws IOException {
        snmp.close();
    }

    static PDU buildSetRequest(SnmpSetRequest request) {
        PDU pdu = new PDU();
        pdu.setType(PDU.SET);
        pdu.add(new VariableBinding(request.getOid(), request.getValue()));
        return pdu;
    }
}
