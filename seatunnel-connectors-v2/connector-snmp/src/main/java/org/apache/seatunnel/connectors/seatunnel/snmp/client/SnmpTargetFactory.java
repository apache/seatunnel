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

package org.apache.seatunnel.connectors.seatunnel.snmp.client;

import org.apache.seatunnel.connectors.seatunnel.snmp.config.SnmpTargetConfig;
import org.apache.seatunnel.connectors.seatunnel.snmp.exception.SnmpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.snmp.exception.SnmpConnectorException;

import org.snmp4j.CommunityTarget;
import org.snmp4j.Target;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;

/** Builds SNMP4J targets without exposing credentials through logs or error messages. */
public final class SnmpTargetFactory {

    private SnmpTargetFactory() {}

    public static Target create(SnmpTargetConfig config) {
        CommunityTarget target = new CommunityTarget();
        try {
            target.setAddress(new UdpAddress(config.getHost() + "/" + config.getPort()));
        } catch (IllegalArgumentException e) {
            throw new SnmpConnectorException(
                    SnmpConnectorErrorCode.INVALID_CONFIG,
                    "Invalid SNMP agent address " + config.getHost() + ":" + config.getPort(),
                    e);
        }
        target.setCommunity(new OctetString(config.getCommunity()));
        target.setVersion(SnmpConstants.version2c);
        target.setTimeout(config.getTimeoutMillis());
        target.setRetries(config.getRetries());
        return target;
    }
}
