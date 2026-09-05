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

package org.apache.seatunnel.engine.common.job;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class JobStatusTest {

    @Test
    @ResourceLock("default-locale")
    void testFromStringUsesLocaleIndependentCaseConversion() {
        Locale originalLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            assertEquals(JobStatus.FINISHED, JobStatus.fromString("finished"));
        } finally {
            Locale.setDefault(originalLocale);
        }
    }

    /**
     * {@code JobStatus}'s ordinal is relied on directly: it is transported raw over the internal
     * RPC (see {@code GetJobStatusOperation}/{@code ClientJobProxy}/{@code JobClient}) and used to
     * index the {@code stateTimestamps} array in {@code PhysicalPlan}. Reordering or inserting a
     * new constant would silently corrupt both without this test failing. If this test breaks
     * because a new state was intentionally added, update the expected array below to match and
     * double check every ordinal-based array index and RPC decode site still lines up.
     */
    @Test
    void testOrdinalTableIsPinned() {
        JobStatus[] expected = {
            JobStatus.INITIALIZING,
            JobStatus.CREATED,
            JobStatus.PENDING,
            JobStatus.SCHEDULED,
            JobStatus.RUNNING,
            JobStatus.FAILING,
            JobStatus.FAILED,
            JobStatus.DOING_SAVEPOINT,
            JobStatus.SAVEPOINT_DONE,
            JobStatus.CANCELING,
            JobStatus.CANCELED,
            JobStatus.FINISHED,
            JobStatus.UNKNOWABLE
        };
        assertArrayEquals(expected, JobStatus.values());
    }
}
