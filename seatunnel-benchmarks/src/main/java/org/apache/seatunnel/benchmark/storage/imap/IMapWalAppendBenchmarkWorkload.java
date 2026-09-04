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

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.hazelcast.map.IMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/** Real FileMapStore WAL appends for growing-key and hot-key histories. */
@State(Scope.Thread)
@AuxCounters(AuxCounters.Type.EVENTS)
public class IMapWalAppendBenchmarkWorkload {

    /** Constants shared with the JMH entry point without becoming auxiliary counter fields. */
    public static final class Batch {
        public static final int APPENDS_PER_INVOCATION = 100;

        private Batch() {}
    }

    private static final long NEW_KEY_BASE = Long.MAX_VALUE / 2;
    private static final long HOT_KEY = Long.MAX_VALUE / 2 - 1;

    @Param({"1", "10", "100"})
    int pipelineCount;

    private final AtomicLong sequence = new AtomicLong();

    private IMap<Long, JobDAGInfo> walMap;
    private JobDAGInfo[] mutations;
    private int historyJobExpireMinutes;
    private Path walRoot;
    private long iterationStartBytes;
    private long iterationAppendCount;
    private long bytesPerAppend;
    private long lastAppendedKey;
    private long lastExpectedJobId;

    /** Builds realistic DAG payload variants outside measured append operations. */
    @Setup(Level.Trial)
    public void setUp(SeaTunnelStorageEnvironmentContext environment) {
        walMap =
                environment
                        .getServer()
                        .getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO);
        historyJobExpireMinutes =
                environment.storageConfig().getEngineConfig().getHistoryJobExpireMinutes();
        String clusterName =
                environment
                        .getServer()
                        .getNodeEngine()
                        .getHazelcastInstance()
                        .getConfig()
                        .getClusterName();
        walRoot =
                environment
                        .imapDirectory()
                        .resolve(clusterName)
                        .resolve(Constant.IMAP_FINISHED_JOB_VERTEX_INFO);
        mutations = new JobDAGInfo[] {createMutation(1L), createMutation(2L)};
    }

    /** Captures a stable storage baseline before one fixed-size append phase. */
    @Setup(Level.Iteration)
    public void setUpIteration() {
        iterationStartBytes = currentWalBytes();
        iterationAppendCount = 0L;
        bytesPerAppend = 0L;
        lastAppendedKey = 0L;
        lastExpectedJobId = 0L;
    }

    /** Validates the final mutation and caches byte growth before the environment is destroyed. */
    @TearDown(Level.Iteration)
    public void tearDownIteration() {
        bytesPerAppend =
                calculateWalBytesPerAppend(
                        iterationStartBytes, currentWalBytes(), iterationAppendCount);
        verifyLastAppend();
    }

    /** Appends a fixed number of unique-key mutations so candidates see the same growth phase. */
    public void appendNewKeyBatch() {
        for (int index = 0; index < Batch.APPENDS_PER_INVOCATION; index++) {
            appendNewKey();
        }
    }

    /** Appends a fixed number of hot-key mutations so candidates see the same history depth. */
    public void appendHotKeyBatch() {
        for (int index = 0; index < Batch.APPENDS_PER_INVOCATION; index++) {
            appendHotKey();
        }
    }

    private void appendNewKey() {
        long mutation = sequence.incrementAndGet();
        JobDAGInfo value = mutations[(int) (mutation & 1L)];
        lastAppendedKey = NEW_KEY_BASE - mutation;
        lastExpectedJobId = value.getJobId();
        walMap.put(lastAppendedKey, value, historyJobExpireMinutes, TimeUnit.MINUTES);
        iterationAppendCount++;
    }

    private void appendHotKey() {
        long mutation = sequence.incrementAndGet();
        JobDAGInfo value = mutations[(int) (mutation & 1L)];
        lastAppendedKey = HOT_KEY;
        lastExpectedJobId = value.getJobId();
        walMap.put(lastAppendedKey, value, historyJobExpireMinutes, TimeUnit.MINUTES);
        iterationAppendCount++;
    }

    /** Returns the cached persisted byte growth per append for the completed JMH iteration. */
    public long walBytesPerAppend() {
        return bytesPerAppend;
    }

    private long currentWalBytes() {
        if (walRoot == null || !Files.exists(walRoot)) {
            return 0L;
        }
        try (Stream<Path> files = Files.walk(walRoot)) {
            return files.filter(path -> path.getFileName().toString().endsWith("wal.txt"))
                    .mapToLong(IMapWalAppendBenchmarkWorkload::fileSize)
                    .sum();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to measure persisted WAL bytes", e);
        }
    }

    void verifyLastAppend() {
        walMap.evict(lastAppendedKey);
        walMap.loadAll(Collections.singleton(lastAppendedKey), true);
        JobDAGInfo persisted = walMap.get(lastAppendedKey);
        if (persisted == null || persisted.getJobId() != lastExpectedJobId) {
            throw new IllegalStateException("The latest WAL append was not durably persisted");
        }
    }

    static long calculateWalBytesPerAppend(long startBytes, long endBytes, long appendCount) {
        if (appendCount <= 0L) {
            throw new IllegalStateException("The WAL append phase did not execute");
        }
        long walByteDelta = endBytes - startBytes;
        if (walByteDelta <= 0L) {
            throw new IllegalStateException("The WAL append phase did not persist any bytes");
        }
        return walByteDelta / appendCount;
    }

    private JobDAGInfo createMutation(long jobId) {
        JobDAGInfo mutation = JobDagFixtureFactory.create(pipelineCount);
        mutation.setJobId(jobId);
        return mutation;
    }

    private static long fileSize(Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read WAL file size " + path, e);
        }
    }
}
