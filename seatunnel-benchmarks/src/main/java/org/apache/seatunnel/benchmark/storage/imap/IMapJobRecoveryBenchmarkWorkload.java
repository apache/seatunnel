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

import org.apache.seatunnel.benchmark.storage.SeaTunnelStorageEnvironmentContext;
import org.apache.seatunnel.benchmark.storage.StorageLifecycleFixtureJob;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.job.JobInfo;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.hazelcast.map.IMap;

/** Full running-job metadata recovery from the production FileMapStore. */
@State(Scope.Thread)
public class IMapJobRecoveryBenchmarkWorkload {

    private static final long RECOVERY_KEY_BASE = Long.MIN_VALUE + 3_000_000L;

    @Param({"100", "1000"})
    public int runningJobCount;

    private StorageLifecycleFixtureJob fixtureJob;
    private IMap<Long, JobInfo> runningJobInfoMap;

    /** Persists realistic JobInfo values and removes their in-memory copies before recovery. */
    @Setup(Level.Trial)
    public void setUp(SeaTunnelStorageEnvironmentContext environment) throws Exception {
        fixtureJob = new StorageLifecycleFixtureJob(environment);
        fixtureJob.start();
        try {
            JobInfo runningJobInfo = fixtureJob.runningJobInfo();
            fixtureJob.finish();

            runningJobInfoMap =
                    environment
                            .getServer()
                            .getNodeEngine()
                            .getHazelcastInstance()
                            .getMap(Constant.IMAP_RUNNING_JOB_INFO);
            runningJobInfoMap.delete(fixtureJob.getJobId());

            // Keep fixture generation single-threaded. IMap.putAll fans entries out across
            // partition threads, while the file-backed WAL currently uses a single producer.
            for (int index = 0; index < runningJobCount; index++) {
                runningJobInfoMap.put(RECOVERY_KEY_BASE + index, runningJobInfo);
            }
            runningJobInfoMap.evictAll();
        } catch (Exception setupFailure) {
            closeFixtureAfterFailedSetup(setupFailure);
            throw setupFailure;
        }
    }

    @Setup(Level.Invocation)
    public void prepareInvocation() {
        runningJobInfoMap.evictAll();
    }

    @TearDown(Level.Invocation)
    public void cleanInvocation() {
        try {
            verifyRecovery();
        } finally {
            runningJobInfoMap.evictAll();
        }
    }

    /** Reloads every persisted JobInfo and scans the same entry set used by master failover. */
    public int recoverRunningJobs() {
        runningJobInfoMap.loadAll(true);
        return runningJobInfoMap.entrySet().size();
    }

    public void verifyRecovery() {
        int recoveredJobCount = runningJobInfoMap.size();
        if (recoveredJobCount != runningJobCount) {
            throw new IllegalStateException(
                    "Recovered "
                            + recoveredJobCount
                            + " running jobs instead of "
                            + runningJobCount);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (fixtureJob != null) {
            fixtureJob.close();
        }
    }

    private void closeFixtureAfterFailedSetup(Exception setupFailure) {
        try {
            fixtureJob.close();
        } catch (Exception cleanupFailure) {
            setupFailure.addSuppressed(cleanupFailure);
        } finally {
            fixtureJob = null;
        }
    }
}
