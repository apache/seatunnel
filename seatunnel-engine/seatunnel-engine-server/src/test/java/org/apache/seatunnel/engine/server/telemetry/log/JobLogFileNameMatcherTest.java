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

package org.apache.seatunnel.engine.server.telemetry.log;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers exact job log filename matching for active, rolled, and unclassified logs.
 *
 * <p>The regression target is avoiding substring matches between adjacent job ids.
 */
public class JobLogFileNameMatcherTest {

    /**
     * Exact matcher must accept only the lifecycle files that belong to the requested job id.
     *
     * <p>Invalid suffixes and adjacent ids must stay outside cleanup and download scope.
     */
    @Test
    void testMatchesOnlyExactJobLogFiles() {
        assertTrue(JobLogFileNameMatcher.isJobLogFile("job-123.log", 123));
        assertTrue(JobLogFileNameMatcher.isJobLogFile("job-123.log.unclassified", 123));
        assertTrue(JobLogFileNameMatcher.isJobLogFile("job-123.log.2026-07-13-1", 123));

        assertFalse(JobLogFileNameMatcher.isJobLogFile("job-1234.log", 123));
        assertFalse(JobLogFileNameMatcher.isJobLogFile("job-123.log.tmp", 123));
        assertFalse(JobLogFileNameMatcher.isJobLogFile("job-123.log.2026-07-13", 123));
        assertFalse(JobLogFileNameMatcher.isJobLogFile("seatunnel.log", 123));
    }

    /**
     * The pruner only touches rolled segments. Active files and unclassified sidecars must stay
     * outside the prune scope so that running jobs keep their current log file intact.
     */
    @Test
    void testIsRolledSegmentRejectsActiveAndUnclassified() {
        assertTrue(JobLogFileNameMatcher.isRolledSegment("job-123.log.2026-07-13-1"));
        assertTrue(JobLogFileNameMatcher.isRolledSegment("job-123.log.2026-08-05-12"));
        assertTrue(JobLogFileNameMatcher.isRolledSegment("/tmp/job-123.log.2026-08-05-12"));

        assertFalse(JobLogFileNameMatcher.isRolledSegment("job-123.log"));
        assertFalse(JobLogFileNameMatcher.isRolledSegment("job-123.log.unclassified"));
        assertFalse(JobLogFileNameMatcher.isRolledSegment("job-123.log.tmp"));
        assertFalse(JobLogFileNameMatcher.isRolledSegment("seatunnel.log.2026-08-05-1"));
        assertFalse(JobLogFileNameMatcher.isRolledSegment("metrics.log.2026-08-05-1"));
        assertFalse(JobLogFileNameMatcher.isRolledSegment("seatunnel.log"));
        assertFalse(JobLogFileNameMatcher.isRolledSegment(null));
    }
}
