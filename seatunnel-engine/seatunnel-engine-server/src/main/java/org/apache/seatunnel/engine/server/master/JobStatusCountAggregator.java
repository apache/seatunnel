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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.master.JobHistoryService.JobState;

import com.hazelcast.aggregation.Aggregator;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Counts retained records on members without transferring their job details. */
final class JobStatusCountAggregator
        implements Aggregator<Map.Entry<Long, JobState>, Map<JobStatus, Long>> {
    private static final long serialVersionUID = 1L;

    // Keep filtering in accumulation: a separate predicate can deserialize each record twice.
    // Hazelcast also serializes this exclusion set in aggregation responses.
    private final Set<Long> activeJobIds;
    private final Map<JobStatus, Long> counts = new EnumMap<>(JobStatus.class);

    JobStatusCountAggregator(Set<Long> activeJobIds) {
        this.activeJobIds = new HashSet<>(activeJobIds);
    }

    @Override
    public void accumulate(Map.Entry<Long, JobState> entry) {
        JobState state = entry.getValue();
        if (!activeJobIds.contains(state.getJobId())) {
            counts.merge(state.getJobStatus(), 1L, Long::sum);
        }
    }

    @Override
    public void combine(Aggregator aggregator) {
        ((JobStatusCountAggregator) aggregator)
                .counts.forEach((status, count) -> counts.merge(status, count, Long::sum));
    }

    @Override
    public Map<JobStatus, Long> aggregate() {
        return counts;
    }
}
