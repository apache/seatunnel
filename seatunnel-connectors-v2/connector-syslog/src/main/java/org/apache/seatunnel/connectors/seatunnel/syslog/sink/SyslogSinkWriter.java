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

package org.apache.seatunnel.connectors.seatunnel.syslog.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.syslog.config.SyslogSinkConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

public final class SyslogSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final SyslogMessageEncoder encoder;
    private final SyslogTlsClient client;
    private final ReentrantLock writeLock = new ReentrantLock();

    SyslogSinkWriter(SyslogSinkConfig config, SeaTunnelRowType rowType) throws IOException {
        encoder = new SyslogMessageEncoder(rowType, config.getMaxMessageBytes());
        client = SyslogTlsClient.connect(config);
    }

    SyslogSinkWriter(SyslogMessageEncoder encoder, SyslogTlsClient client) {
        this.encoder = encoder;
        this.client = client;
    }

    @Override
    public void write(SeaTunnelRow row) throws IOException {
        if (!writeLock.tryLock()) {
            throw new IOException("Syslog concurrent writes are not supported");
        }
        try {
            client.write(encoder.encode(row));
        } finally {
            writeLock.unlock();
        }
    }

    /** Writes are synchronous; this is a local flush, not an acknowledgment from the receiver. */
    @Override
    public Optional<Void> prepareCommit() {
        try {
            client.flush();
            return Optional.empty();
        } catch (IOException e) {
            // AbstractSinkWriter's legacy overload cannot declare IOException.
            throw new java.io.UncheckedIOException(e);
        }
    }

    @Override
    public Optional<Void> prepareCommit(long checkpointId) throws IOException {
        client.flush();
        return Optional.empty();
    }

    @Override
    public List<Void> snapshotState(long checkpointId) throws IOException {
        client.flush();
        return Collections.emptyList();
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
