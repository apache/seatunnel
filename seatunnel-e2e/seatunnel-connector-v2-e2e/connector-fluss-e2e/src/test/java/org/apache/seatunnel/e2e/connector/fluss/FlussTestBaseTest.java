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
package org.apache.seatunnel.e2e.connector.fluss;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.admin.ListOffsetsResult;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(60)
class FlussTestBaseTest {

    private static final TablePath TABLE_PATH = TablePath.of("test_db", "test_table");

    @Test
    void retriesBootstrapDisconnect() throws Exception {
        Connection connection = readyConnection();
        AtomicInteger attempts = new AtomicInteger();

        FlussTestBase.waitForBucketLeaders(
                TABLE_PATH,
                () -> {
                    if (attempts.getAndIncrement() == 0) {
                        throw new IllegalStateException("Bootstrap server disconnected");
                    }
                    return connection;
                });

        assertEquals(2, attempts.get());
        verify(connection).close();
    }

    @Test
    void retriesTableMetadataFailureWithFreshConnection() throws Exception {
        Connection first = readyConnection();
        Connection second = readyConnection();
        CompletableFuture<TableInfo> failure = new CompletableFuture<>();
        failure.completeExceptionally(
                new IllegalStateException("Metadata temporarily unavailable"));
        when(first.getAdmin().getTableInfo(TABLE_PATH)).thenReturn(failure);
        AtomicInteger attempts = new AtomicInteger();

        FlussTestBase.waitForBucketLeaders(
                TABLE_PATH, () -> attempts.getAndIncrement() == 0 ? first : second);

        assertEquals(2, attempts.get());
        verify(first).close();
        verify(second).close();
    }

    @Test
    void retriesMissingBucketLeaderWithFreshConnection() throws Exception {
        Connection first = readyConnection();
        Connection second = readyConnection();
        CompletableFuture<Long> failure = new CompletableFuture<>();
        failure.completeExceptionally(new IllegalStateException("Bucket leader not ready"));
        when(first.getAdmin().listOffsets(eq(TABLE_PATH), any(), any()))
                .thenReturn(new ListOffsetsResult(Collections.singletonMap(0, failure)));
        AtomicInteger attempts = new AtomicInteger();

        FlussTestBase.waitForBucketLeaders(
                TABLE_PATH, () -> attempts.getAndIncrement() == 0 ? first : second);

        assertEquals(2, attempts.get());
        verify(first).close();
        verify(second).close();
    }

    private static Connection readyConnection() {
        Connection connection = mock(Connection.class);
        Admin admin = mock(Admin.class);
        TableInfo tableInfo = mock(TableInfo.class);
        when(connection.getAdmin()).thenReturn(admin);
        when(tableInfo.getNumBuckets()).thenReturn(1);
        when(admin.getTableInfo(TABLE_PATH))
                .thenReturn(CompletableFuture.completedFuture(tableInfo));
        when(admin.listOffsets(eq(TABLE_PATH), any(), any()))
                .thenReturn(
                        new ListOffsetsResult(
                                Collections.singletonMap(
                                        0, CompletableFuture.completedFuture(1L))));
        return connection;
    }
}
