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

package org.apache.seatunnel.engine.server.dag.physical;

import java.io.Serializable;
import java.util.List;

/**
 * The persisted restore bookkeeping of one pipeline: how many restores it already consumed and why
 * each of them happened.
 *
 * <p>SubPlan keeps this in memory only, but a master failover rebuilds every SubPlan from scratch.
 * Without persisting it, the {@code job.retry.times} budget restarts at zero on every master
 * switchover, so a crash-looping pipeline is restarted indefinitely, and the recorded restore
 * causes are lost exactly when they are most needed. SubPlan therefore writes this record to the
 * running-job-state IMap on every restore and reads it back when it is reconstructed.
 *
 * <p>The value is stored in an IMap of a Hazelcast cluster whose members run the same engine
 * version, so plain Java serialization is sufficient here.
 */
public class PipelineRestoreInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int restoreCount;

    private final List<String> restoreCauses;

    public PipelineRestoreInfo(int restoreCount, List<String> restoreCauses) {
        this.restoreCount = restoreCount;
        this.restoreCauses = restoreCauses;
    }

    public int getRestoreCount() {
        return restoreCount;
    }

    public List<String> getRestoreCauses() {
        return restoreCauses;
    }
}
