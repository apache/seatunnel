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

package org.apache.seatunnel.connectors.seatunnel.coap;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

/** CoAP UDP resource listener with a single reader and bounded in-memory admission. */
public final class CoapSource
        implements SeaTunnelSource<SeaTunnelRow, CoapSource.Split, CoapSource.EnumeratorState> {
    private static final long serialVersionUID = 1L;
    private final CoapSettings.Listener settings;
    private final CatalogTable table;
    private final String fingerprint;

    CoapSource(CoapSettings.Listener settings) {
        this.settings = settings;
        this.table =
                settings.typed
                        ? CatalogTableUtil.buildWithConfig(CoapOptions.IDENTIFIER, settings.options)
                        : defaultTable(settings);
        this.fingerprint = fingerprint(settings, table);
    }

    private static CatalogTable defaultTable(CoapSettings.Listener settings) {
        SeaTunnelDataType<?> type =
                settings.format == CoapOptions.Format.BINARY
                                || (settings.format == CoapOptions.Format.DATAGRAM
                                        && settings.datagramMode
                                                == CoapOptions.DatagramMode.RAW_DATA
                                        && settings.options.get(CoapOptions.DATAGRAM_SEPARATOR)
                                                == null)
                        ? PrimitiveByteArrayType.INSTANCE
                        : BasicType.STRING_TYPE;
        return CatalogTableUtil.getCatalogTable(
                CoapOptions.IDENTIFIER,
                new SeaTunnelRowType(new String[] {"payload"}, new SeaTunnelDataType<?>[] {type}));
    }

    private static String fingerprint(CoapSettings.Listener config, CatalogTable table) {
        String original =
                config.address.getHostAddress()
                        + "|"
                        + config.port
                        + "|"
                        + config.path
                        + "|"
                        + config.format
                        + "|"
                        + config.typed
                        + "|"
                        + table.getTablePath()
                        + "|"
                        + table.getSeaTunnelRowType()
                        + "|"
                        + config.limit
                        + "|"
                        + config.csvDelimiter
                        + "|"
                        + config.avroSchema
                        + "|"
                        + config.protoMessage
                        + "|"
                        + config.protoSchema
                        + "|"
                        + config.datagramMode
                        + "|"
                        + config.options.get(CoapOptions.DATAGRAM_SEPARATOR)
                        + "|"
                        + config.options.get(CoapOptions.XML_PATH)
                        + "|"
                        + new java.util.TreeMap<>(config.options.get(CoapOptions.XML_NAMESPACES))
                        + "|"
                        + config.options.get(CoapOptions.RECORD_HEADER_FIELD)
                        + "|"
                        + config.options.get(CoapOptions.XML_PRESERVE_ROOT)
                        + "|"
                        + config.options.get(CoapOptions.LOG_MODE)
                        + "|"
                        + config.options.get(CoapOptions.LOG_PATTERN)
                        + "|"
                        + new java.util.TreeMap<>(config.options.get(CoapOptions.LOG_GROUPS))
                        + "|"
                        + new java.util.TreeMap<>(config.options.get(CoapOptions.GROK_DEFINITIONS))
                        + "|"
                        + new java.util.TreeMap<>(config.options.get(CoapOptions.COLLECTD_TYPES))
                        + "|"
                        + config.options.get(CoapOptions.COLLECTD_CONVERT_TIME)
                        + "|"
                        + config.options.get(CoapOptions.COLLECTD_OMIT_INTERVAL);
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(original.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    @Override
    public String getPluginName() {
        return CoapOptions.IDENTIFIER;
    }

    @Override
    public Boundedness getBoundedness() {
        return settings.limit > 0 ? Boundedness.BOUNDED : Boundedness.UNBOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(table);
    }

    @Override
    public SourceReader<SeaTunnelRow, Split> createReader(SourceReader.Context context) {
        if (context.getIndexOfSubtask() != 0) {
            throw CoapSettings.invalid("only source reader 0 can own the UDP listener");
        }
        return new CoapReader(context, settings, table, fingerprint);
    }

    @Override
    public SourceSplitEnumerator<Split, EnumeratorState> createEnumerator(
            SourceSplitEnumerator.Context<Split> context) {
        return new Enumerator(
                context, fingerprint, new EnumeratorState(fingerprint, new Split(fingerprint, 0)));
    }

    @Override
    public SourceSplitEnumerator<Split, EnumeratorState> restoreEnumerator(
            SourceSplitEnumerator.Context<Split> context, EnumeratorState state) {
        return new Enumerator(context, fingerprint, state);
    }

    /** Immutable progress for the single listener, without replayable datagrams. */
    static final class Split implements SourceSplit {
        private static final long serialVersionUID = 1L;
        final String fingerprint;
        final long emitted;

        Split(String fingerprint, long emitted) {
            this.fingerprint = fingerprint;
            this.emitted = emitted;
        }

        @Override
        public String splitId() {
            return "coap-listener";
        }
    }

    /** Identifies the listener contract and tracks a split until a reader owns it. */
    static final class EnumeratorState implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String fingerprint;
        private final Split unassigned;

        EnumeratorState(String fingerprint, Split unassigned) {
            this.fingerprint = fingerprint;
            this.unassigned = unassigned;
        }
    }

    /** Reassigns an owned split only when the engine returns it on recovery. */
    private static final class Enumerator implements SourceSplitEnumerator<Split, EnumeratorState> {
        private final Context<Split> context;
        private final String fingerprint;
        private Split pending;
        private boolean running;

        private Enumerator(Context<Split> context, String fingerprint, EnumeratorState state) {
            if (context.currentParallelism() != 1) {
                throw CoapSettings.invalid("source parallelism must be 1");
            }
            if (!fingerprint.equals(state.fingerprint)) {
                throw CoapSettings.invalid("listener settings changed during checkpoint restore");
            }
            this.context = context;
            this.fingerprint = fingerprint;
            this.pending = state.unassigned;
        }

        @Override
        public void open() {}

        @Override
        public synchronized void run() {
            running = true;
            assign();
        }

        @Override
        public synchronized void close() {
            running = false;
        }

        @Override
        public synchronized void addSplitsBack(List<Split> splits, int reader) {
            if (splits.isEmpty()) {
                return;
            }
            if (reader != 0
                    || splits.size() != 1
                    || !fingerprint.equals(splits.get(0).fingerprint)) {
                throw CoapSettings.invalid("unexpected listener checkpoint split");
            }
            pending = splits.get(0);
            assign();
        }

        @Override
        public synchronized int currentUnassignedSplitSize() {
            return pending == null ? 0 : 1;
        }

        @Override
        public synchronized void handleSplitRequest(int reader) {
            assign();
        }

        @Override
        public synchronized void registerReader(int reader) {
            assign();
        }

        @Override
        public synchronized EnumeratorState snapshotState(long id) {
            return new EnumeratorState(fingerprint, pending);
        }

        private void assign() {
            if (running && pending != null && context.registeredReaders().contains(0)) {
                context.assignSplit(0, pending);
                pending = null;
                context.signalNoMoreSplits(0);
            }
        }
    }
}
