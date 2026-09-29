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

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.snmp.config.SnmpSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.snmp.exception.SnmpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.snmp.exception.SnmpConnectorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.function.LongSupplier;

/** Polls a configured SNMP agent and emits one row for each requested OID. */
public class SnmpSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private static final Logger LOG = LoggerFactory.getLogger(SnmpSourceReader.class);
    private static final long MAX_IDLE_WAIT_MILLIS = 200L;

    private final SnmpSourceConfig config;
    private final SingleSplitReaderContext context;
    private final SnmpClientFactory clientFactory;
    private final LongSupplier currentTimeMillis;

    private volatile boolean closed;
    private SnmpClient client;
    private long nextPollTimeMillis;

    /** Creates a reader for the supplied SNMP agent configuration. */
    public SnmpSourceReader(SnmpSourceConfig config, SingleSplitReaderContext context) {
        this(config, context, Snmp4jClient::new, System::currentTimeMillis);
    }

    SnmpSourceReader(
            SnmpSourceConfig config,
            SingleSplitReaderContext context,
            SnmpClientFactory clientFactory,
            LongSupplier currentTimeMillis) {
        this.config = config;
        this.context = context;
        this.clientFactory = clientFactory;
        this.currentTimeMillis = currentTimeMillis;
    }

    @Override
    public void open() {
        try {
            client = clientFactory.create(config);
            LOG.info(
                    "SNMP source reader opened for agent [{}:{}] with [{}] OIDs",
                    config.getHost(),
                    config.getPort(),
                    config.getOids().size());
        } catch (IOException e) {
            throw new SnmpConnectorException(
                    SnmpConnectorErrorCode.CONNECTION_FAILED,
                    "Failed to initialize SNMP client for agent "
                            + config.getHost()
                            + ":"
                            + config.getPort(),
                    e);
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (closed || noMoreSplits) {
            return;
        }

        if (Boundedness.UNBOUNDED.equals(context.getBoundedness())) {
            long waitMillis = nextPollTimeMillis - currentTimeMillis.getAsLong();
            if (waitMillis > 0) {
                Thread.sleep(Math.min(waitMillis, MAX_IDLE_WAIT_MILLIS));
                return;
            }
        }

        long pollTime = currentTimeMillis.getAsLong();
        List<SnmpRecord> records;
        try {
            records = client.get(config.getOids());
        } catch (IOException e) {
            if (closed) {
                return;
            }
            throw new SnmpConnectorException(
                    SnmpConnectorErrorCode.POLL_FAILED,
                    "Failed to poll SNMP agent " + config.getHost() + ":" + config.getPort(),
                    e);
        }

        if (closed) {
            return;
        }
        synchronized (output.getCheckpointLock()) {
            for (SnmpRecord record : records) {
                output.collect(
                        new SeaTunnelRow(
                                new Object[] {
                                    config.getHost() + ":" + config.getPort(),
                                    record.getOid(),
                                    record.getValue(),
                                    record.getValueType(),
                                    pollTime
                                }));
            }
        }

        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            noMoreSplits = true;
            context.signalNoMoreElement();
        } else {
            nextPollTimeMillis = currentTimeMillis.getAsLong() + config.getPollIntervalMillis();
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        if (client != null) {
            client.close();
        }
        LOG.info("SNMP source reader closed for agent [{}:{}]", config.getHost(), config.getPort());
    }

    interface SnmpClientFactory {
        SnmpClient create(SnmpSourceConfig config) throws IOException;
    }
}
