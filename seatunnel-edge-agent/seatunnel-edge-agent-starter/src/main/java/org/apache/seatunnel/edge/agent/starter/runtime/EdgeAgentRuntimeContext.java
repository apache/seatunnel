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

package org.apache.seatunnel.edge.agent.starter.runtime;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.edge.agent.connector.EdgeInputReader;
import org.apache.seatunnel.edge.agent.connector.EdgeSourcePositionStore;
import org.apache.seatunnel.edge.agent.starter.wal.SqliteAgentRuntimeStore;
import org.apache.seatunnel.edge.agent.starter.wal.WalStore;
import org.apache.seatunnel.edge.agent.transport.EdgeCollectorTransport;
import org.apache.seatunnel.edge.agent.transport.serialize.PayloadSerializer;

import lombok.Getter;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

@Getter
public class EdgeAgentRuntimeContext {

    private final EdgeInputReader reader;
    private final SqliteAgentRuntimeStore sqliteRuntime;
    private final WalStore walStore;
    private final EdgeSourcePositionStore sourcePositionStore;
    private final EdgeCollectorTransport transport;
    private final PayloadSerializer payloadSerializer;
    private final AtomicBoolean running;

    public EdgeAgentRuntimeContext(
            EdgeInputReader reader,
            SqliteAgentRuntimeStore sqliteRuntime,
            EdgeCollectorTransport transport,
            PayloadSerializer payloadSerializer,
            AtomicBoolean running) {
        this.reader = Objects.requireNonNull(reader, "reader");
        this.sqliteRuntime = Objects.requireNonNull(sqliteRuntime, "sqliteRuntime");
        this.walStore = sqliteRuntime.walStore();
        this.sourcePositionStore = sqliteRuntime.sourcePositionStore();
        this.transport = Objects.requireNonNull(transport, "transport");
        this.payloadSerializer = Objects.requireNonNull(payloadSerializer, "payloadSerializer");
        this.running = running != null ? running : new AtomicBoolean(true);
    }

    @VisibleForTesting
    public EdgeAgentRuntimeContext(
            EdgeInputReader reader,
            WalStore walStore,
            EdgeSourcePositionStore sourcePositionStore,
            EdgeCollectorTransport transport,
            PayloadSerializer payloadSerializer,
            AtomicBoolean running) {
        this.reader = Objects.requireNonNull(reader, "reader");
        this.sqliteRuntime = null;
        this.walStore = Objects.requireNonNull(walStore, "walStore");
        this.sourcePositionStore = sourcePositionStore;
        this.transport = Objects.requireNonNull(transport, "transport");
        this.payloadSerializer = Objects.requireNonNull(payloadSerializer, "payloadSerializer");
        this.running = running != null ? running : new AtomicBoolean(true);
    }
}
