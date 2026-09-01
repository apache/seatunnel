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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Code-built JobDAGInfo values and isolated IMap DAG operations for the JMH entry point. */
@State(Scope.Thread)
public class IMapDagStorageBenchmarkWorkload {

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
    private long storeKey;

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

        Map<Long, JobDAGInfo> storedDags = new HashMap<>(storedDagCount);
        for (int index = 0; index < storedDagCount; index++) {
            storedDags.put(PRESSURE_KEY_BASE + index, finishedJobDag);
        }
        finishedJobDagMap.putAll(storedDags);
        finishedJobDagMap.put(LOAD_KEY, finishedJobDag);
        finishedJobDagMap.evict(LOAD_KEY);
    }

    /** Persists and evicts one DAG before measuring the MapStore reload path. */
    @Setup(Level.Invocation)
    public void prepareInvocation() {
        long invocation = sequence.incrementAndGet();
        storeKey = Long.MAX_VALUE - invocation;
        finishedJobDagMap.evict(LOAD_KEY);
    }

    @TearDown(Level.Invocation)
    public void cleanInvocation() {
        finishedJobDagMap.delete(storeKey);
        finishedJobDagMap.evict(LOAD_KEY);
    }

    public JobDAGInfo storeFinishedJobDag() {
        return finishedJobDagMap.put(
                storeKey, finishedJobDag, historyJobExpireMinutes, TimeUnit.MINUTES);
    }

    public void loadFinishedJobDag() {
        finishedJobDagMap.loadAll(Collections.singleton(LOAD_KEY), true);
    }

    public void verifyFinishedJobDagLoaded() {
        if (!finishedJobDagMap.containsKey(LOAD_KEY)) {
            throw new IllegalStateException("The persisted JobDAGInfo was not loaded");
        }
    }
}
