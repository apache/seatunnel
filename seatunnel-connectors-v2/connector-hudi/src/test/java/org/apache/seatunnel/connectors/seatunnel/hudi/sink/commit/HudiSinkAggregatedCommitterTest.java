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

package org.apache.seatunnel.connectors.seatunnel.hudi.sink.commit;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSemantics;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.state.HudiAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.state.HudiCommitInfo;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.WriteOperationType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * The commit info of a checkpoint is the state of the aggregated committer, so it has to survive
 * the serialization of the checkpoint, and the writers that have nothing to commit must not be
 * committed.
 */
class HudiSinkAggregatedCommitterTest {

    private static final String TABLE_NAME = "hudi";

    private static final String INSTANT_TIME = "20240101120000000";

    private static final long CHECKPOINT_ID = 5L;

    private static final SeaTunnelRowType SEA_TUNNEL_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "name"},
                    new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});

    private HudiSinkAggregatedCommitter committer;

    @BeforeEach
    void setUp() {
        committer =
                new HudiSinkAggregatedCommitter(sinkConfig(), tableConfig(), SEA_TUNNEL_ROW_TYPE);
    }

    @Test
    void shouldSkipTheWritersThatHaveNothingToCommit() {
        HudiCommitInfo pendingInstant = commitInfo();
        HudiCommitInfo writerWithoutData = new HudiCommitInfo(CHECKPOINT_ID, null, null);

        HudiAggregatedCommitInfo aggregatedCommitInfo =
                committer.combine(
                        Arrays.asList(
                                pendingInstant, null, writerWithoutData, new HudiCommitInfo()));

        Assertions.assertEquals(1, aggregatedCommitInfo.getCommitInfos().size());
        Assertions.assertEquals(
                INSTANT_TIME, aggregatedCommitInfo.getCommitInfos().get(0).getInstantTime());
    }

    @Test
    void shouldSurviveTheCheckpointSerialization() throws Exception {
        Serializer<HudiAggregatedCommitInfo> serializer = new DefaultSerializer<>();
        HudiAggregatedCommitInfo aggregatedCommitInfo =
                new HudiAggregatedCommitInfo(Collections.singletonList(commitInfo()));

        HudiAggregatedCommitInfo restored =
                serializer.deserialize(serializer.serialize(aggregatedCommitInfo));

        Assertions.assertEquals(1, restored.getCommitInfos().size());
        HudiCommitInfo restoredCommitInfo = restored.getCommitInfos().get(0);
        Assertions.assertEquals(CHECKPOINT_ID, restoredCommitInfo.getCheckpointId());
        Assertions.assertEquals(INSTANT_TIME, restoredCommitInfo.getInstantTime());
        Assertions.assertEquals(1, restoredCommitInfo.getWriteStatuses().size());
    }

    private HudiCommitInfo commitInfo() {
        return new HudiCommitInfo(
                CHECKPOINT_ID, INSTANT_TIME, Collections.singletonList(new WriteStatus()));
    }

    private HudiSinkConfig sinkConfig() {
        return HudiSinkConfig.builder()
                .tableDfsPath("/tmp/hudi")
                .tableList(Collections.singletonList(tableConfig()))
                .semantics(HudiSemantics.EXACTLY_ONCE)
                .build();
    }

    private HudiTableConfig tableConfig() {
        return HudiTableConfig.builder()
                .tableName(TABLE_NAME)
                .database("default")
                .opType(WriteOperationType.INSERT)
                .build();
    }
}
