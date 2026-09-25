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

package org.apache.seatunnel.lineage;

import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LineageEventTest {

    @Test
    void addsRunPropertyWithoutDisturbingTheRestOfTheEvent() {
        LineageEvent event = event();

        LineageEvent withProperty = event.withRunProperty("flink_job_id", "abc123");

        assertEquals("abc123", withProperty.runProperties().get("flink_job_id"));
        assertEquals("kept", withProperty.runProperties().get("existing"));
        assertEquals(event.runId(), withProperty.runId());
        assertEquals(event.eventType(), withProperty.eventType());
        assertEquals(event.eventTime(), withProperty.eventTime());
        assertEquals(event.outputs(), withProperty.outputs());

        // The source event must stay immutable so a shared event can be reused per callback.
        assertFalse(event.runProperties().containsKey("flink_job_id"));
    }

    /**
     * The Flink status hook has no job identifier for a callback that does not carry one, so a null
     * value must be a no-op rather than something the caller has to branch on. Emitting a null
     * property would also send a null into the run facet.
     */
    @Test
    void ignoresAbsentRunPropertyValues() {
        LineageEvent event = event();

        assertSame(event, event.withRunProperty("flink_job_id", null));
        assertSame(event, event.withRunProperty(null, "abc123"));
        assertSame(event, event.withRunProperty("  ", "abc123"));
    }

    @Test
    void runPropertiesStayImmutable() {
        LineageEvent event = event().withRunProperty("flink_job_id", "abc123");

        assertThrows(
                UnsupportedOperationException.class,
                () -> event.runProperties().put("injected", "value"));
    }

    @Test
    void changingEventTypeKeepsRunIdentityAndProperties() {
        LineageEvent event = event().withRunProperty("flink_job_id", "abc123");

        LineageEvent completed = event.withEventType(LineageEventType.COMPLETE);

        assertEquals(LineageEventType.COMPLETE, completed.eventType());
        assertEquals(event.runId(), completed.runId());
        assertEquals("abc123", completed.runProperties().get("flink_job_id"));
        assertTrue(completed.eventTime().getOffset().equals(ZoneOffset.UTC));
    }

    private static LineageEvent event() {
        return LineageEvent.builder()
                .runId(UUID.nameUUIDFromBytes("seatunnel-job:1".getBytes()))
                .eventTime(ZonedDateTime.now(ZoneOffset.UTC))
                .eventType(LineageEventType.START)
                .jobNamespace("seatunnel")
                .jobName("job")
                .producer("https://seatunnel.apache.org/")
                .runFacet(LineageConfig.DEFAULT_RUN_FACET)
                .runProperties(Collections.singletonMap("existing", "kept"))
                .outputs(
                        Collections.singletonList(
                                LineageDataset.of("mysql://host:3306", "db.orders")))
                .build();
    }
}
