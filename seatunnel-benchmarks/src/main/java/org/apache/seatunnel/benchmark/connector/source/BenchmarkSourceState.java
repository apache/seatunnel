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

package org.apache.seatunnel.benchmark.connector.source;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Checkpoint state for the benchmark split enumerator.
 *
 * <p>Record progress is stored in {@link BenchmarkSourceSplit} by the reader checkpoint and is
 * returned to the enumerator through {@code addSplitsBack}. This state only retains the shared
 * schedule origin and assignment bookkeeping.
 */
public final class BenchmarkSourceState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long startEpochMillis;
    private final Set<Integer> assignedSubtasks;

    public BenchmarkSourceState(long startEpochMillis, Set<Integer> assignedSubtasks) {
        this.startEpochMillis = startEpochMillis;
        this.assignedSubtasks = new HashSet<>(assignedSubtasks);
    }

    public long getStartEpochMillis() {
        return startEpochMillis;
    }

    public Set<Integer> getAssignedSubtasks() {
        return new HashSet<>(assignedSubtasks);
    }
}
