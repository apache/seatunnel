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
package org.apache.seatunnel.connectors.seatunnel.basecamp;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Checks scoped Link parsing, initial checkpoint ownership and frozen recording dates.
 *
 * <p>The dedicated Zeta module covers HTTPS paging, credential refresh and nested recovery.
 */
class BasecampCheckpointTest {
    private static final String FEED = "/123/projects/recordings.json";

    @Test
    void keepsAnOpaqueCommaInTheUriAndRestoresOmittedFeedSelectors() {
        Map<String, String> controls = selectors();
        String result =
                BasecampLink.next(
                        BasecampResource.TODOS,
                        FEED,
                        controls,
                        List.of(
                                "<https://ignored.example.invalid/page>; rel=prev; title=\"words, preserved\"",
                                "<?cursor=a,b%2Bc>; title=\"quote: \\\"here\\\", comma\"; rel=\"next alternate\""));
        assertEquals("?cursor=a,b%2Bc&type=Todo&sort=updated_at&direction=desc", result);
        assertNull(
                BasecampLink.next(
                        BasecampResource.TODOS,
                        FEED,
                        controls,
                        List.of("<?page=1>; rel=\"prev\"")));
    }

    @Test
    void rejectsNextLinksWithDifferentScopeOrAnAmbiguousRelation() {
        for (List<String> headers :
                List.of(
                        List.of("<https://outside.example.invalid" + FEED + "?page=2>; rel=next"),
                        List.of("</124/projects/recordings.json?page=2>; rel=next"),
                        List.of("</123/projects.json?page=2>; rel=next"),
                        List.of("<?type=Message>; rel=next"),
                        List.of("<?type=Todo&ty%70e=Todo>; rel=next"),
                        List.of("<?page=2&access_token=fixture>; rel=next"),
                        List.of("<?page=2>; rel=\"next next\""),
                        List.of("<?page=2>; rel=next; anchor=\"https://outside.example.invalid\""),
                        List.of("<?page=2>; rel=next, <?page=3>; rel=next"),
                        List.of("<?page=2>; title=\"unterminated; rel=next"))) {
            assertThrows(
                    BasecampResource.Failure.class,
                    () -> BasecampLink.next(BasecampResource.TODOS, FEED, selectors(), headers));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void restoresThePreassignmentScanWithRotatedCredentialsAndTheSameAccount() throws Exception {
        SourceSplitEnumerator.Context<BasecampSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        Map<String, Object> options = settings("123", "fixture-initial-token");
        BasecampSplit prepared =
                new BasecampSource(ReadonlyConfig.fromMap(options))
                        .createEnumerator(context)
                        .snapshotState(1);
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                ObjectOutputStream state = new ObjectOutputStream(bytes)) {
            state.writeObject(prepared);
            state.flush();
            assertFalse(
                    bytes.toString(StandardCharsets.ISO_8859_1).contains("fixture-initial-token"));
        }
        when(context.registeredReaders()).thenReturn(Set.of(0, 1));
        options.put("token", "fixture-rotated-token");
        new BasecampSource(ReadonlyConfig.fromMap(options))
                .restoreEnumerator(context, prepared)
                .run();
        ArgumentCaptor<BasecampSplit> assigned = ArgumentCaptor.forClass(BasecampSplit.class);
        verify(context).assignSplit(eq(0), assigned.capture());
        verify(context).signalNoMoreSplits(1);
        assertEquals(prepared.end, assigned.getValue().end);
        assertEquals(BasecampResource.PROJECTS, assigned.getValue().phase);
        options.put("account_id", "124");
        assertThrows(
                IllegalStateException.class,
                () ->
                        new BasecampSource(ReadonlyConfig.fromMap(options))
                                .restoreEnumerator(context, prepared));
    }

    @Test
    void freezesRecordingBoundsWithOffsetsAndKeepsAnEqualStartAndEnd() {
        Map<String, Object> values = settings("123", "fixture-token");
        values.put("resources", List.of("todos"));
        values.put("start_date", "2024-01-01T03:00:00+03:00");
        values.put("end_date", "2024-01-01T00:00:00.000Z");
        BasecampConfig config = new BasecampConfig(ReadonlyConfig.fromMap(values));
        BasecampSplit split = config.newSplit(Instant.parse("2024-01-02T00:00:00Z"));
        assertEquals(Instant.parse("2024-01-01T00:00:00Z"), split.start);
        assertEquals(split.start, split.end);
        assertFalse(split.finished());
        assertTrue(config.newSplit(Instant.parse("2023-12-31T23:59:59.999Z")).finished());
    }

    @Test
    void keepsParentAndChildStateDetachedWithoutReconstructingACursorFromAnId() {
        Map<String, Object> values = settings("123", "fixture-token");
        values.put("resources", List.of("schedule_entries"));
        BasecampConfig config = new BasecampConfig(ReadonlyConfig.fromMap(values));
        BasecampSplit progress = config.newSplit(Instant.parse("2024-01-02T00:00:00Z"));
        progress.projectId = "100";
        progress.top.digest = "project-signature";
        progress.scheduleId = "200";
        progress.dockDigest = "dock-signature";
        progress.entries.advance(
                BasecampResource.SCHEDULE_ENTRIES,
                new BasecampApi.Page(List.of(), "?cursor=x%2By", null, "unused"),
                config.maxPages);
        progress.entries.digest = "entry-page";
        progress.entries.row = 1;
        BasecampSplit saved = progress.copy();
        progress.completeSchedule();
        progress.completeProject();
        saved.validate(config);
        assertEquals(1, saved.entries.page);
        assertEquals(1, saved.entries.row);
        assertEquals("?cursor=x%2By", saved.entries.query);
        assertEquals("100", saved.projectId);
        assertEquals("200", saved.scheduleId);
        assertNull(progress.projectId);
    }

    private static Map<String, String> selectors() {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("type", "Todo");
        values.put("sort", "updated_at");
        values.put("direction", "desc");
        return values;
    }

    private static Map<String, Object> settings(String account, String token) {
        return new HashMap<>(
                Map.of(
                        "account_id",
                        account,
                        "token",
                        token,
                        "user_agent",
                        "Basecamp connector tests (test@example.invalid)",
                        "resources",
                        List.of("projects")));
    }
}
