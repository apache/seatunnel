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
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils;
import org.apache.seatunnel.engine.serializer.api.Serializer;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.hazelcast.map.IMap;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/** File-backed IMap recovery with independently controlled key count and WAL mutation history. */
@State(Scope.Thread)
public class IMapWalRecoveryBenchmarkWorkload {

    private static final long RECOVERY_KEY_BASE = Long.MIN_VALUE + 2_000_000L;
    private static final String WAL_FILE_NAME = "wal.txt";

    @Param({"100", "1000"})
    public int uniqueKeyCount;

    @Param({"1", "10", "100"})
    public int mutationsPerKey;

    private IMap<Long, JobDAGInfo> recoveryMap;

    /** Builds a WAL whose total records and latest live key count can vary independently. */
    @Setup(Level.Trial)
    public void setUp(SeaTunnelStorageEnvironmentContext environment) throws IOException {
        recoveryMap =
                environment
                        .getServer()
                        .getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO);

        writeRecoveryWal(environment);
        recoveryMap.evictAll();
    }

    @Setup(Level.Invocation)
    public void prepareInvocation() {
        recoveryMap.evictAll();
    }

    @TearDown(Level.Invocation)
    public void cleanInvocation() {
        try {
            verifyRecovery();
        } finally {
            recoveryMap.evictAll();
        }
    }

    public void recoverAll() {
        recoveryMap.loadAll(true);
    }

    public void verifyRecovery() {
        if (recoveryMap.size() != uniqueKeyCount) {
            throw new IllegalStateException(
                    "Recovered " + recoveryMap.size() + " keys instead of " + uniqueKeyCount);
        }
        JobDAGInfo latest = recoveryMap.get(RECOVERY_KEY_BASE);
        if (latest == null || latest.getJobId() != mutationsPerKey) {
            throw new IllegalStateException("Recovery did not retain the latest WAL mutation");
        }
        JobDAGInfo last = recoveryMap.get(RECOVERY_KEY_BASE + uniqueKeyCount - 1L);
        if (last == null || last.getJobId() != mutationsPerKey) {
            throw new IllegalStateException("Recovery did not retain every latest WAL mutation");
        }
    }

    private void writeRecoveryWal(SeaTunnelStorageEnvironmentContext environment)
            throws IOException {
        String clusterName =
                environment
                        .getServer()
                        .getNodeEngine()
                        .getHazelcastInstance()
                        .getConfig()
                        .getClusterName();
        Path walFile =
                environment
                        .imapDirectory()
                        .resolve(clusterName)
                        .resolve(Constant.IMAP_FINISHED_JOB_VERTEX_INFO)
                        .resolve("benchmark-recovery")
                        .resolve(WAL_FILE_NAME);
        Files.createDirectories(walFile.getParent());

        Serializer serializer = new ProtoStuffSerializer();
        byte[][] serializedKeys = new byte[uniqueKeyCount][];
        for (int index = 0; index < uniqueKeyCount; index++) {
            serializedKeys[index] = serializer.serialize(RECOVERY_KEY_BASE + index);
        }

        try (OutputStream output = new BufferedOutputStream(Files.newOutputStream(walFile))) {
            for (int mutation = 1; mutation <= mutationsPerKey; mutation++) {
                JobDAGInfo value = JobDagFixtureFactory.create(1);
                value.setJobId((long) mutation);
                byte[] serializedValue = serializer.serialize(value);
                for (byte[] serializedKey : serializedKeys) {
                    IMapFileData record =
                            IMapFileData.builder()
                                    .deleted(false)
                                    .key(serializedKey)
                                    .keyClassName(Long.class.getName())
                                    .value(serializedValue)
                                    .valueClassName(JobDAGInfo.class.getName())
                                    .timestamp(mutation)
                                    .build();
                    output.write(WALDataUtils.wrapperBytes(serializer.serialize(record)));
                }
            }
        }
    }
}
