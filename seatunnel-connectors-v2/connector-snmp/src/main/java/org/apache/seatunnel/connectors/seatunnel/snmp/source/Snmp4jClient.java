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

import org.apache.seatunnel.connectors.seatunnel.snmp.config.SnmpSourceConfig;

import org.snmp4j.CommunityTarget;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.Variable;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultUdpTransportMapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** SNMPv2c client backed by SNMP4J. */
final class Snmp4jClient implements SnmpClient {

    private final SnmpSourceConfig config;
    private final Snmp snmp;
    private final Target target;

    Snmp4jClient(SnmpSourceConfig config) throws IOException {
        this(config, new Snmp(new DefaultUdpTransportMapping()));
    }

    Snmp4jClient(SnmpSourceConfig config, Snmp snmp) throws IOException {
        this.config = config;
        this.target = buildTarget(config);
        try {
            snmp.listen();
        } catch (IOException e) {
            try {
                snmp.close();
            } catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
        this.snmp = snmp;
    }

    @Override
    public List<SnmpRecord> get(List<OID> oids) throws IOException {
        ResponseEvent event = snmp.send(buildGetRequest(oids), target);
        if (event == null || event.getResponse() == null) {
            throw new IOException(
                    "SNMP request timed out for agent "
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
        return extractRecords(response);
    }

    @Override
    public void close() throws IOException {
        snmp.close();
    }

    static PDU buildGetRequest(List<OID> oids) {
        PDU pdu = new PDU();
        pdu.setType(PDU.GET);
        for (OID oid : oids) {
            pdu.add(new VariableBinding(oid));
        }
        return pdu;
    }

    static Target buildTarget(SnmpSourceConfig config) {
        CommunityTarget target = new CommunityTarget();
        target.setAddress(new UdpAddress(config.getHost() + "/" + config.getPort()));
        target.setCommunity(new OctetString(config.getCommunity()));
        target.setVersion(SnmpConstants.version2c);
        target.setTimeout(config.getTimeoutMillis());
        target.setRetries(config.getRetries());
        return target;
    }

    static List<SnmpRecord> extractRecords(PDU response) {
        List<SnmpRecord> records = new ArrayList<>(response.size());
        for (VariableBinding binding : response.getVariableBindings()) {
            Variable variable = binding.getVariable();
            records.add(
                    new SnmpRecord(
                            binding.getOid().toString(),
                            variable.toString(),
                            variable.getSyntaxString()));
        }
        return records;
    }
}
