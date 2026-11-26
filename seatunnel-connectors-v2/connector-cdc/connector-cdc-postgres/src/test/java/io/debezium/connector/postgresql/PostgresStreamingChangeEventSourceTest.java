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

package io.debezium.connector.postgresql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.WalPositionLocator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PostgresStreamingChangeEventSourceTest {

    @Test
    public void shouldSkipRecoveryPositionSearchForBoundedBackfill() {
        PostgresOffsetContext offsetContext = mock(PostgresOffsetContext.class);
        when(offsetContext.getStreamingStoppingLsn()).thenReturn(Lsn.valueOf(200L));

        WalPositionLocator locator =
                PostgresStreamingChangeEventSource.createWalPositionLocator(
                        offsetContext, Lsn.valueOf(100L));

        Assertions.assertFalse(locator.searchingEnabled());
    }

    @Test
    public void shouldKeepRecoveryPositionSearchForUnboundedStreaming() {
        PostgresOffsetContext offsetContext = mock(PostgresOffsetContext.class);

        WalPositionLocator locator =
                PostgresStreamingChangeEventSource.createWalPositionLocator(
                        offsetContext, Lsn.valueOf(100L));

        Assertions.assertTrue(locator.searchingEnabled());
    }
}
