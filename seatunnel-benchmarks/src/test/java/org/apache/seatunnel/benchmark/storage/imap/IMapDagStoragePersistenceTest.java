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

package org.apache.seatunnel.benchmark.storage.imap;

import org.apache.seatunnel.benchmark.dag.JobDagFixtureFactory;
import org.apache.seatunnel.benchmark.storage.SeaTunnelStorageEnvironmentContext;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.imap.storage.file.common.WALReader;
import org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.runner.IterationType;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises complete DAG batches through the benchmark's actual file-backed MapStore. */
class IMapDagStoragePersistenceTest {

    @ParameterizedTest
    @CsvSource({"1,0", "1,100", "10,0", "10,100", "100,0", "100,100"})
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void persistsCompleteBatchesAndDeletionRecords(int pipelines, int retainedDags)
            throws Exception {
        SeaTunnelStorageEnvironmentContext environment = new SeaTunnelStorageEnvironmentContext();
        try {
            environment.setUp();
            IMapDagStorageBenchmarkWorkload workload = new IMapDagStorageBenchmarkWorkload();
            workload.pipelineCount = pipelines;
            workload.storedDagCount = retainedDags;
            workload.setUp(environment);
            JobDAGInfo expected = JobDagFixtureFactory.create(pipelines);
            String clusterName =
                    environment
                            .getServer()
                            .getNodeEngine()
                            .getHazelcastInstance()
                            .getConfig()
                            .getClusterName();
            Path walRoot =
                    new Path(
                            environment
                                    .imapDirectory()
                                    .resolve(clusterName)
                                    .resolve(Constant.IMAP_FINISHED_JOB_VERTEX_INFO)
                                    .toUri());
            try (FileSystem fileSystem = FileSystem.newInstanceLocal(new Configuration())) {
                WALReader reader =
                        new WALReader(
                                fileSystem, FileConfiguration.HDFS, new ProtoStuffSerializer());
                Set<Object> retainedKeys =
                        new HashSet<>(reader.loadAllData(walRoot, Collections.emptySet()).keySet());
                assertEquals(retainedDags + 1, retainedKeys.size());
                for (int batch = 0; batch < 2; batch++) {
                    workload.prepareStoreIteration(
                            new IterationParams(
                                    IterationType.MEASUREMENT, 2, TimeValue.seconds(1), 1));
                    workload.prepareInvocation();
                    long lastKey = workload.storeFinishedJobDagBatch();
                    workload.cleanInvocation();

                    // Read the WAL independently of IMap's cache, once for the entire batch.
                    Map<Object, Object> persisted =
                            reader.loadAllData(walRoot, Collections.emptySet());
                    assertEquals(retainedDags + 101, persisted.size());
                    for (long key = lastKey; key < lastKey + 100; key++) {
                        assertEquals(expected, persisted.get(key), "Missing or invalid DAG " + key);
                    }
                    workload.cleanStoreIteration();
                    Map<Object, Object> afterDeletion =
                            reader.loadAllData(walRoot, Collections.emptySet());
                    assertEquals(retainedKeys, afterDeletion.keySet());
                    assertTrue(afterDeletion.values().stream().allMatch(expected::equals));
                    for (long key = lastKey; key < lastKey + 100; key++) {
                        assertFalse(afterDeletion.containsKey(key));
                    }
                }
                for (int invocation = 0; invocation < 2; invocation++) {
                    workload.prepareInvocation();
                    workload.loadFinishedJobDag();
                    workload.cleanInvocation();
                }
            }
        } finally {
            environment.tearDown();
        }
    }
}
