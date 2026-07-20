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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.offset;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.SerializationUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;

import java.sql.ResultSet;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LsnOffsetFactoryTest {

    @Test
    public void shouldCreateRestartableOffsetFromCommittedLsn() {
        LsnOffset offset = LsnOffsetFactory.toLsnOffset("0/16B6C50");
        Map<String, String> offsetMap = offset.getOffset();

        Assertions.assertEquals(
                String.valueOf(Lsn.valueOf("0/16B6C50").asLong()),
                offsetMap.get(SourceInfo.LSN_KEY));
        Assertions.assertEquals(
                offsetMap.get(SourceInfo.LSN_KEY),
                offsetMap.get(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY));
        Assertions.assertEquals(
                offsetMap.get(SourceInfo.LSN_KEY),
                offsetMap.get(PostgresOffsetContext.LAST_COMMIT_LSN_KEY));
    }

    @Test
    public void shouldRoundTripSerializedLsnOffset() {
        LsnOffset offset = LsnOffsetFactory.toLsnOffset("0/16B6C50");

        LsnOffset restored = SerializationUtils.deserialize(SerializationUtils.serialize(offset));

        Assertions.assertEquals(offset, restored);
        Assertions.assertEquals(offset.getOffset(), restored.getOffset());
    }

    @Test
    public void shouldRejectReplicationSlotWithNullCommittedLsn() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString(1)).thenReturn(null);
        when(resultSet.wasNull()).thenReturn(true);

        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> LsnOffsetFactory.readCommittedOffset(resultSet, "empty_slot"));

        Assertions.assertTrue(exception.getMessage().contains("empty_slot"));
        Assertions.assertTrue(exception.getMessage().contains("confirmed_flush_lsn"));
    }

    @Test
    public void shouldRejectMissingReplicationSlot() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.next()).thenReturn(false);

        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> LsnOffsetFactory.readCommittedOffset(resultSet, "missing_slot"));

        Assertions.assertTrue(exception.getMessage().contains("missing_slot"));
        Assertions.assertTrue(exception.getMessage().contains("does not exist"));
    }

    @Test
    public void shouldRejectActiveReplicationSlot() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getObject(2)).thenReturn(1234);

        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> LsnOffsetFactory.readCommittedOffset(resultSet, "active_slot"));

        Assertions.assertTrue(exception.getMessage().contains("active_slot"));
        Assertions.assertTrue(exception.getMessage().contains("1234"));
        Assertions.assertTrue(exception.getMessage().contains("other consumer"));
    }
}
