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

package org.apache.seatunnel.translation.flink.sink;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.connector.sink2.StandardSinkTopologies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

class RecoveryTopologyTest {
    @Test
    void testExistingOperatorIdsRemainUnchanged() {
        Assertions.assertEquals(operatorIds(false), operatorIds(true));
    }

    private Map<String, String> operatorIds(boolean patched) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<CommittableMessage<CommitWrapper<String>>> input =
                env.fromElements(1)
                        .map(
                                value ->
                                        (CommittableMessage<CommitWrapper<String>>)
                                                new CommittableWithLineage<>(
                                                        new CommitWrapper<>("prepared"), 1L, 0))
                        .returns(
                                TypeInformation.of(
                                        new TypeHint<
                                                CommittableMessage<CommitWrapper<String>>>() {}))
                        .name("Commit input");
        StandardSinkTopologies.addGlobalCommitter(
                patched ? new RecoveryAwareSink.RecoveryDataStream<>(input) : input,
                EmptyCommitter::new,
                TestSerializer::new);
        JobGraph graph = env.getStreamGraph().getJobGraph();
        Map<String, String> ids = new TreeMap<>();
        for (JobVertex vertex : graph.getVertices()) {
            ids.put(
                    vertex.getName(),
                    vertex.getOperatorIDs().stream()
                            .map(pair -> pair.getGeneratedOperatorID().toString())
                            .collect(Collectors.joining(",")));
        }
        return ids;
    }

    private static final class EmptyCommitter implements Committer<CommitWrapper<String>> {
        public void commit(Collection<Committer.CommitRequest<CommitWrapper<String>>> requests) {}

        public void close() {}
    }

    private static final class TestSerializer
            implements SimpleVersionedSerializer<CommitWrapper<String>> {
        public int getVersion() {
            return 0;
        }

        public byte[] serialize(CommitWrapper<String> value) {
            return new byte[0];
        }

        public CommitWrapper<String> deserialize(int version, byte[] bytes) {
            return new CommitWrapper<>("prepared");
        }
    }
}
