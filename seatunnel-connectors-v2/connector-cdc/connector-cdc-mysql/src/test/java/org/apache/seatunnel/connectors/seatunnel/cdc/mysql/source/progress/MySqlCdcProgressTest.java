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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.progress;

import org.apache.seatunnel.api.cdc.CdcProgressPhase;
import org.apache.seatunnel.api.cdc.CdcProgressSnapshot;
import org.apache.seatunnel.api.cdc.CdcProgressSupportGroup;
import org.apache.seatunnel.api.cdc.CdcProgressSupportLevel;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.Collections;

public class MySqlCdcProgressTest {

    @Test
    public void shouldPreserveMySqlBinlogPositionInIncrementalProgress() {
        BinlogOffset offset =
                new BinlogOffset(
                        "mysql-bin.000003",
                        784L,
                        2L,
                        5L,
                        1710000000L,
                        "3E11FA47-71CA-11E1-9E33-C80AA9429562:23",
                        1001);
        IncrementalSplit split =
                new IncrementalSplit(
                        "mysql-incremental",
                        Collections.singletonList(new TableId("inventory", null, "orders")),
                        offset,
                        BinlogOffset.NO_STOPPING_OFFSET,
                        Collections.emptyList());

        CdcProgressSnapshot snapshot = MySqlCdcProgress.forIncrementalSplit(split, 2000L);

        Assertions.assertEquals(MySqlCdcProgress.CONNECTOR_TYPE, snapshot.getConnectorType());
        Assertions.assertEquals(CdcProgressPhase.INCREMENTAL, snapshot.getPhase());
        Assertions.assertEquals("mysql-incremental", snapshot.getSplitId());
        Assertions.assertEquals(
                "mysql-bin.000003", snapshot.getRawPosition().getValues().get("file"));
        Assertions.assertEquals("784", snapshot.getRawPosition().getValues().get("pos"));
        Assertions.assertEquals(
                "3E11FA47-71CA-11E1-9E33-C80AA9429562:23",
                snapshot.getRawPosition().getValues().get("gtids"));
        Assertions.assertEquals("1001", snapshot.getRawPosition().getValues().get("server_id"));
        Assertions.assertEquals(
                snapshot.getRawPosition(),
                snapshot.getIncrementalProgress().getCurrentConsumedPosition());
        Assertions.assertEquals(
                CdcProgressSupportLevel.EXACT,
                snapshot.getSupportLevels().get(CdcProgressSupportGroup.RAW_POSITION));
    }
}
