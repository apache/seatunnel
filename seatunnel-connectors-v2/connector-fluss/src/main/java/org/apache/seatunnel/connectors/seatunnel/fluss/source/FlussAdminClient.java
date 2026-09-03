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
package org.apache.seatunnel.connectors.seatunnel.fluss.source;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.TemporaryClassLoaderContext;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.admin.OffsetSpec;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TableInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
class FlussAdminClient implements AutoCloseable {

    private final Connection connection;
    private final Admin admin;
    private final String description;

    FlussAdminClient(Configuration flussConfig, String description) {
        this.description = description;
        Connection conn = createConnection(flussConfig);
        try {
            this.admin = conn.getAdmin();
        } catch (RuntimeException e) {
            try {
                conn.close();
            } catch (Exception closeError) {
                e.addSuppressed(closeError);
            }
            throw e;
        }
        this.connection = conn;
    }

    /**
     * Creates the Fluss connection with the connector classloader pinned as the thread context
     * classloader. Fluss authenticates by loading its 'PLAINTEXT' protocol plugin via {@code
     * ServiceLoader} off the context classloader (AuthenticationFactory); a reader creates this
     * client on the framework's SplitFetcher thread, whose context classloader is not the
     * connector's. The connection's authenticator is loaded once here and reused by every later
     * admin / scan call, so pinning only the connection setup is enough.
     */
    private static Connection createConnection(Configuration flussConfig) {
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(FlussAdminClient.class.getClassLoader())) {
            return ConnectionFactory.createConnection(flussConfig);
        }
    }

    Connection connection() {
        return connection;
    }

    TableInfo getTableInfo(TablePath tablePath) {
        try {
            return admin.getTableInfo(toFlussTablePath(tablePath)).get();
        } catch (Exception e) {
            throw wrap(
                    String.format(
                            "Failed to read Fluss table info for %s", tablePath.getFullName()),
                    e);
        }
    }

    Map<Integer, Long> earliestOffsets(TablePath tablePath, List<Integer> buckets) {
        CompletableFuture<Map<Integer, Long>> earliest =
                listOffsetsAsync(tablePath, buckets, new OffsetSpec.EarliestSpec());
        return awaitOffsets(earliest, tablePath, buckets);
    }

    Map<Integer, Long> latestOffsets(TablePath tablePath, List<Integer> buckets) {
        CompletableFuture<Map<Integer, Long>> latest =
                listOffsetsAsync(tablePath, buckets, new OffsetSpec.LatestSpec());
        return awaitOffsets(latest, tablePath, buckets);
    }

    BucketBounds bucketBounds(TablePath tablePath, List<Integer> buckets) {
        CompletableFuture<Map<Integer, Long>> earliest =
                listOffsetsAsync(tablePath, buckets, new OffsetSpec.EarliestSpec());
        CompletableFuture<Map<Integer, Long>> latest =
                listOffsetsAsync(tablePath, buckets, new OffsetSpec.LatestSpec());
        return new BucketBounds(
                awaitOffsets(earliest, tablePath, buckets),
                awaitOffsets(latest, tablePath, buckets));
    }

    private CompletableFuture<Map<Integer, Long>> listOffsetsAsync(
            TablePath tablePath, List<Integer> buckets, OffsetSpec spec) {
        return admin.listOffsets(toFlussTablePath(tablePath), buckets, spec).all();
    }

    private Map<Integer, Long> awaitOffsets(
            CompletableFuture<Map<Integer, Long>> future,
            TablePath tablePath,
            List<Integer> buckets) {
        try {
            return future.get();
        } catch (Exception e) {
            throw wrap(
                    String.format(
                            "Failed to list offsets for table %s buckets %s",
                            tablePath.getFullName(), buckets),
                    e);
        }
    }

    static final class BucketBounds {
        final Map<Integer, Long> earliest;
        final Map<Integer, Long> latest;

        BucketBounds(Map<Integer, Long> earliest, Map<Integer, Long> latest) {
            this.earliest = earliest;
            this.latest = latest;
        }
    }

    private static IllegalStateException wrap(String message, Exception e) {
        // Restore the interrupt status so an interrupt during a blocking get() (e.g. job
        // cancellation) is not swallowed by wrapping it in an unchecked exception.
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        return new IllegalStateException(message, e);
    }

    private static com.alibaba.fluss.metadata.TablePath toFlussTablePath(TablePath tablePath) {
        return com.alibaba.fluss.metadata.TablePath.of(
                tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    public void close() {
        // Best-effort: this client only backs short-lived schema/offset discovery, so a failure to
        // release it must not fail a job (or a discovery) whose data was already fetched. Both the
        // admin and the connection are always attempted; failures are logged, not thrown.
        // Pin the connector classloader (see createConnection): teardown may run on a framework
        // thread whose context classloader is not the connector's, and Fluss can lazily load
        // classes while closing.
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(FlussAdminClient.class.getClassLoader())) {
            try {
                admin.close();
            } catch (Exception e) {
                log.warn("Failed to close Fluss admin for {}", description, e);
            }
            try {
                connection.close();
            } catch (Exception e) {
                log.warn("Failed to close Fluss connection for {}", description, e);
            }
        }
    }
}
