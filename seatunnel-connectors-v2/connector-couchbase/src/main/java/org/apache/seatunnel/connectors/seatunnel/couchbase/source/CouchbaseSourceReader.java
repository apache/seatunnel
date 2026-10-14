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

package org.apache.seatunnel.connectors.seatunnel.couchbase.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.couchbase.config.CouchbaseSourceOptions;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.codec.DefaultJsonSerializer;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Iterator;
import java.util.stream.Stream;

public class CouchbaseSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    // Preserve the query response's number representations for SeaTunnel's JSON converter.
    static final JsonSerializer RAW_JSON_SERIALIZER =
            new JsonSerializer() {
                private final JsonSerializer delegate = DefaultJsonSerializer.create();

                @Override
                public byte[] serialize(Object value) {
                    return delegate.serialize(value);
                }

                @Override
                public <T> T deserialize(Class<T> target, byte[] bytes) {
                    if (target == byte[].class) {
                        return target.cast(bytes);
                    }
                    return delegate.deserialize(target, bytes);
                }
            };
    private final ReadonlyConfig config;
    private final CatalogTable catalogTable;
    private final SingleSplitReaderContext context;
    private final JsonDeserializationSchema deserializer;
    private volatile boolean closed;
    private volatile Cluster cluster;
    private volatile Stream<byte[]> rows;
    private final Sinks.Empty<Void> cancellation = Sinks.empty();

    CouchbaseSourceReader(
            ReadonlyConfig config, CatalogTable table, SingleSplitReaderContext context) {
        this.config = config;
        this.catalogTable = table;
        this.context = context;
        this.deserializer = new JsonDeserializationSchema(table, false, false);
    }

    /** Connect on the worker and release the client if readiness fails. */
    @Override
    public void open() {
        Cluster connected =
                Cluster.connect(
                        config.get(CouchbaseSourceOptions.CONNECTION_STRING),
                        config.get(CouchbaseSourceOptions.USERNAME),
                        config.get(CouchbaseSourceOptions.PASSWORD));
        synchronized (this) {
            if (closed) {
                connected.disconnect();
                return;
            }
            cluster = connected;
        }
        try {
            connected
                    .bucket(config.get(CouchbaseSourceOptions.BUCKET))
                    .waitUntilReady(
                            Duration.ofSeconds(config.get(CouchbaseSourceOptions.READY_TIMEOUT)));
        } catch (RuntimeException failure) {
            closeAfterFailure(failure);
            throw failure;
        }
    }

    /** Consume a backpressured query without buffering the entire collection. */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        Cluster connected = cluster;
        if (closed || noMoreSplits) {
            return;
        }
        try {
            Stream<byte[]> stream =
                    connected
                            .reactive()
                            .query(
                                    statement(config),
                                    QueryOptions.queryOptions()
                                            .serializer(RAW_JSON_SERIALIZER)
                                            .readonly(true)
                                            .scanConsistency(QueryScanConsistency.REQUEST_PLUS)
                                            .timeout(
                                                    Duration.ofSeconds(
                                                            config.get(
                                                                    CouchbaseSourceOptions
                                                                            .QUERY_TIMEOUT))))
                            .flatMapMany(result -> result.rowsAs(byte[].class))
                            .takeUntilOther(cancellation.asMono())
                            .toStream(1);
            synchronized (this) {
                if (closed) {
                    stream.close();
                    return;
                }
                rows = stream;
            }
            try (Stream<byte[]> ignored = stream) {
                Iterator<byte[]> iterator = stream.iterator();
                while (!closed && iterator.hasNext()) {
                    JsonNode document = deserializer.deserializeToJsonNode(iterator.next());
                    if (document == null || !document.isObject()) {
                        throw new IllegalArgumentException(
                                "Couchbase source requires JSON object documents");
                    }
                    SeaTunnelRow row = deserializer.convertToRowData(document);
                    row.setTableId(catalogTable.getTablePath().toString());
                    synchronized (output.getCheckpointLock()) {
                        if (closed) {
                            return;
                        }
                        output.collect(row);
                    }
                }
                if (!closed) {
                    noMoreSplits = true;
                    context.signalNoMoreElement();
                }
            } finally {
                rows = null;
            }
        } catch (Exception failure) {
            if (closed) {
                return;
            }
            closeAfterFailure(failure);
            throw failure;
        }
    }

    private void closeAfterFailure(Exception failure) {
        try {
            close();
        } catch (RuntimeException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    static String statement(ReadonlyConfig config) {
        return "SELECT RAW d FROM "
                + quote(config.get(CouchbaseSourceOptions.BUCKET))
                + "."
                + quote(config.get(CouchbaseSourceOptions.SCOPE))
                + "."
                + quote(config.get(CouchbaseSourceOptions.COLLECTION))
                + " AS d";
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    /** Cancel any in-flight query before closing its owned SDK connection. */
    @Override
    public synchronized void close() {
        closed = true;
        cancellation.tryEmitEmpty();
        try {
            if (rows != null) {
                Stream<byte[]> current = rows;
                rows = null;
                current.close();
            }
        } finally {
            if (cluster != null) {
                Cluster connected = cluster;
                cluster = null;
                connected.disconnect();
            }
        }
    }
}
