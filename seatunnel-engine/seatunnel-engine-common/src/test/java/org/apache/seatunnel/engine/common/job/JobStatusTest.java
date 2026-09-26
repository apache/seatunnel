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
     * Pins the exact order of {@link JobStatus#values()} so that reordering or inserting a constant
     * fails this test loudly instead of silently corrupting the two things that rely on the ordinal
     * within a single build: the raw {@code int} sent over the internal RPC (see {@code
     * GetJobStatusOperation}/{@code ClientJobProxy}/{@code JobClient}) and the index into the
     * {@code stateTimestamps} array in {@code PhysicalPlan}.
     *
     * <p><b>This guard only covers same-build code.</b> It says nothing about a rolling upgrade
     * where a client and coordinator run different builds with a reordered or differently-sized
     * {@code JobStatus}: decoding the raw ordinal back via {@code JobStatus.values()[ordinal]} at
     * {@code ClientJobProxy}/{@code JobClient} can still throw {@code
     * ArrayIndexOutOfBoundsException} or silently resolve to the wrong constant in that scenario.
     * Closing that gap needs a name-based, versioned RPC transport, which is a separate, larger
     * design item and out of scope here.
     *
     * <p>If this test fails because a new state was intentionally added or reordered, update the
     * expected array below to match, and audit every ordinal-based site named above by hand.
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
        JobStatus[] actual = JobStatus.values();
        assertEquals(
                expected.length, actual.length, "JobStatus constant count changed unexpectedly");
        for (int ordinal = 0; ordinal < expected.length; ordinal++) {
            assertEquals(
                    expected[ordinal],
                    actual[ordinal],
                    "JobStatus ordinal " + ordinal + " drifted from the pinned constant");
        }
    }
}
