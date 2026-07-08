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

package org.apache.seatunnel.engine.server.master.cleanup;

import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class PipelineCleanupRecordTest {

    @Test
    void testIsCleanedWithEmptyTaskGroups() {
        PipelineCleanupRecord record =
                new PipelineCleanupRecord(
                        new PipelineLocation(1L, 1),
                        PipelineStatus.FINISHED,
                        false,
                        Collections.emptyMap(),
                        Collections.emptySet(),
                        true,
                        System.currentTimeMillis(),
                        0L,
                        0);
        Assertions.assertTrue(record.isCleaned());
    }

    @Test
    void testIsCleanedRequiresMetricsCleanedAndAllTaskGroupsCleaned() {
        PipelineLocation pipelineLocation = new PipelineLocation(1L, 1);
        TaskGroupLocation taskGroupLocation1 = new TaskGroupLocation(1L, 1, 1L);
        TaskGroupLocation taskGroupLocation2 = new TaskGroupLocation(1L, 1, 2L);

        Map<TaskGroupLocation, Address> taskGroups = new HashMap<>();
        taskGroups.put(taskGroupLocation1, null);
        taskGroups.put(taskGroupLocation2, null);

        PipelineCleanupRecord record =
                new PipelineCleanupRecord(
                        pipelineLocation,
                        PipelineStatus.CANCELED,
                        false,
                        taskGroups,
                        new HashSet<>(),
                        false,
                        System.currentTimeMillis(),
                        0L,
                        0);

        Assertions.assertFalse(record.isCleaned());

        record.setMetricsImapCleaned(true);
        Assertions.assertFalse(record.isCleaned());

        record.setCleanedTaskGroups(Collections.singleton(taskGroupLocation1));
        Assertions.assertFalse(record.isCleaned());

        record.setCleanedTaskGroups(new HashSet<>(taskGroups.keySet()));
        Assertions.assertTrue(record.isCleaned());
    }

    @Test
    void testMergeFromPrefersNonNullFieldsAndUnionsCollections() {
        PipelineLocation pipelineLocation1 = new PipelineLocation(1L, 1);
        PipelineLocation pipelineLocation2 = new PipelineLocation(1L, 2);
        TaskGroupLocation taskGroupLocation1 = new TaskGroupLocation(1L, 1, 1L);
        TaskGroupLocation taskGroupLocation2 = new TaskGroupLocation(1L, 1, 2L);

        Map<TaskGroupLocation, Address> taskGroups1 = new HashMap<>();
        taskGroups1.put(taskGroupLocation1, null);
        Set<TaskGroupLocation> cleaned1 = new HashSet<>();
        cleaned1.add(taskGroupLocation1);

        PipelineCleanupRecord record1 =
                new PipelineCleanupRecord(
                        pipelineLocation1,
                        PipelineStatus.FINISHED,
                        false,
                        taskGroups1,
                        cleaned1,
                        false,
                        100L,
                        200L,
                        1);

        Map<TaskGroupLocation, Address> taskGroups2 = new HashMap<>();
        taskGroups2.put(taskGroupLocation2, null);
        Set<TaskGroupLocation> cleaned2 = new HashSet<>();
        cleaned2.add(taskGroupLocation2);

        PipelineCleanupRecord record2 =
                new PipelineCleanupRecord(
                        pipelineLocation2,
                        PipelineStatus.CANCELED,
                        true,
                        taskGroups2,
                        cleaned2,
                        true,
                        300L,
                        400L,
                        3);

        PipelineCleanupRecord merged = record1.mergeFrom(record2);

        Assertions.assertEquals(pipelineLocation1, merged.getPipelineLocation());
        Assertions.assertEquals(PipelineStatus.FINISHED, merged.getFinalStatus());
        Assertions.assertTrue(merged.isSavepointEnd());
        Assertions.assertTrue(merged.isMetricsImapCleaned());

        Assertions.assertEquals(2, merged.getTaskGroups().size());
        Assertions.assertTrue(merged.getTaskGroups().containsKey(taskGroupLocation1));
        Assertions.assertTrue(merged.getTaskGroups().containsKey(taskGroupLocation2));

        Assertions.assertEquals(2, merged.getCleanedTaskGroups().size());
        Assertions.assertTrue(merged.getCleanedTaskGroups().contains(taskGroupLocation1));
        Assertions.assertTrue(merged.getCleanedTaskGroups().contains(taskGroupLocation2));

        Assertions.assertEquals(100L, merged.getCreateTimeMillis());
        Assertions.assertEquals(400L, merged.getLastAttemptTimeMillis());
        Assertions.assertEquals(3, merged.getAttemptCount());
    }
}
