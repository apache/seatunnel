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

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.hazelcast.map.IMap;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Code-built JobDAGInfo values and isolated IMap DAG operations for the JMH entry point. */
@State(Scope.Thread)
public class IMapDagStorageBenchmarkWorkload {

    public static final int STORE_OPERATIONS_PER_INVOCATION = 100;

    private static final long PRESSURE_KEY_BASE = Long.MIN_VALUE + 1_000_000L;
    private static final long LOAD_KEY = Long.MIN_VALUE + 1L;

    @Param({"1", "10", "100"})
    public int pipelineCount;

    @Param({"0", "100"})
    public int storedDagCount;

    private final AtomicLong sequence = new AtomicLong();

    private IMap<Long, JobDAGInfo> finishedJobDagMap;
    private JobDAGInfo finishedJobDag;
    private int historyJobExpireMinutes;
    private long[] storeKeys;
    private int storedDagCountInIteration;
    private boolean loadExecuted;

    /** Constructs an exact number of production source-to-sink DAG pipelines. */
    @Setup(Level.Trial)
    public void setUp(SeaTunnelStorageEnvironmentContext environment) {
        finishedJobDagMap =
                environment
                        .getServer()
                        .getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO);
        finishedJobDag = JobDagFixtureFactory.create(pipelineCount);
        historyJobExpireMinutes =
                environment.storageConfig().getEngineConfig().getHistoryJobExpireMinutes();

        // Keep fixture generation single-threaded. IMap.putAll fans entries out across partition
        // threads, while the file-backed WAL currently uses a single producer.
        for (int index = 0; index < storedDagCount; index++) {
            finishedJobDagMap.put(PRESSURE_KEY_BASE + index, finishedJobDag);
        }
        finishedJobDagMap.put(LOAD_KEY, finishedJobDag);
        finishedJobDagMap.evict(LOAD_KEY);
    }

    /** Builds a fresh, fixed-size key set before each measured DAG-store phase. */
    @Setup(Level.Iteration)
    public void prepareStoreIteration() {
        storeKeys = new long[STORE_OPERATIONS_PER_INVOCATION];
        for (int index = 0; index < STORE_OPERATIONS_PER_INVOCATION; index++) {
            storeKeys[index] = Long.MAX_VALUE - sequence.incrementAndGet();
        }
        storedDagCountInIteration = 0;
    }

    /** Evicts the load fixture before every measured MapStore reload. */
    @Setup(Level.Invocation)
    public void prepareInvocation() {
        loadExecuted = false;
        finishedJobDagMap.evict(LOAD_KEY);
    }

    @TearDown(Level.Invocation)
    public void cleanInvocation() {
        try {
            if (loadExecuted) {
                verifyFinishedJobDagLoaded();
            }
        } finally {
            finishedJobDagMap.evict(LOAD_KEY);
        }
    }

    /** Validates durable samples and removes the fixed store batch outside measured time. */
    @TearDown(Level.Iteration)
    public void cleanStoreIteration() {
        if (storedDagCountInIteration == 0) {
            return;
        }
        try {
            if (storedDagCountInIteration != STORE_OPERATIONS_PER_INVOCATION) {
                throw new IllegalStateException(
                        "The JobDAGInfo store phase did not persist the expected entry count");
            }
            verifyStoredJobDags();
        } finally {
            int cleanupCount = Math.min(storedDagCountInIteration, storeKeys.length);
            for (int index = 0; index < cleanupCount; index++) {
                finishedJobDagMap.delete(storeKeys[index]);
            }
        }
    }

    public long storeFinishedJobDagBatch() {
        for (long storeKey : storeKeys) {
            finishedJobDagMap.put(
                    storeKey, finishedJobDag, historyJobExpireMinutes, TimeUnit.MINUTES);
            storedDagCountInIteration++;
        }
        return storeKeys[STORE_OPERATIONS_PER_INVOCATION - 1];
    }

    public void loadFinishedJobDag() {
        finishedJobDagMap.loadAll(Collections.singleton(LOAD_KEY), true);
        loadExecuted = true;
    }

    public void verifyFinishedJobDagLoaded() {
        JobDAGInfo loaded = finishedJobDagMap.get(LOAD_KEY);
        if (!finishedJobDag.equals(loaded)) {
            throw new IllegalStateException("The persisted JobDAGInfo was not loaded");
        }
    }

    private void verifyStoredJobDags() {
        Set<Long> sampledKeys = new LinkedHashSet<>();
        sampledKeys.add(storeKeys[0]);
        sampledKeys.add(storeKeys[STORE_OPERATIONS_PER_INVOCATION / 2]);
        sampledKeys.add(storeKeys[STORE_OPERATIONS_PER_INVOCATION - 1]);
        for (long sampledKey : sampledKeys) {
            finishedJobDagMap.evict(sampledKey);
        }
        finishedJobDagMap.loadAll(sampledKeys, true);
        for (long sampledKey : sampledKeys) {
            JobDAGInfo stored = finishedJobDagMap.get(sampledKey);
            if (!finishedJobDag.equals(stored)) {
                throw new IllegalStateException(
                        "The JobDAGInfo append was not durably persisted for key " + sampledKey);
            }
        }
    }
}
