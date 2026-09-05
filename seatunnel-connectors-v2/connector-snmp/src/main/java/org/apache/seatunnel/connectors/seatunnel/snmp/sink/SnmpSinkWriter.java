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
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.snmp.config.SnmpSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.snmp.exception.SnmpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.snmp.exception.SnmpConnectorException;

import java.io.IOException;

/** Writes each row as one synchronous SNMPv2c SET request. */
public final class SnmpSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final SnmpSinkConfig config;
    private final SnmpSinkRowConverter converter;
    private final SnmpSetClient client;

    public SnmpSinkWriter(SnmpSinkConfig config, SeaTunnelRowType rowType) {
        this(config, rowType, Snmp4jSetClient::new);
    }

    SnmpSinkWriter(
            SnmpSinkConfig config, SeaTunnelRowType rowType, SnmpSetClientFactory clientFactory) {
        this.config = config;
        this.converter = new SnmpSinkRowConverter(config, rowType);
        try {
            this.client = clientFactory.create(config);
        } catch (IOException e) {
            throw new SnmpConnectorException(
                    SnmpConnectorErrorCode.CONNECTION_FAILED,
                    "Failed to initialize SNMP SET client for agent "
                            + config.getHost()
                            + ":"
                            + config.getPort(),
                    e);
        }
    }

    @Override
    public void write(SeaTunnelRow row) {
        SnmpSetRequest request = converter.convert(row);
        try {
            client.set(request);
        } catch (IOException e) {
            throw new SnmpConnectorException(
                    SnmpConnectorErrorCode.WRITE_FAILED,
                    "Failed to set OID "
                            + request.getOid()
                            + " on SNMP agent "
                            + config.getHost()
                            + ":"
                            + config.getPort(),
                    e);
        }
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            throw new SnmpConnectorException(
                    SnmpConnectorErrorCode.CLOSE_FAILED,
                    "Failed to close SNMP SET client for agent "
                            + config.getHost()
                            + ":"
                            + config.getPort(),
                    e);
        }
    }

    interface SnmpSetClientFactory {
        SnmpSetClient create(SnmpSinkConfig config) throws IOException;
    }
}
