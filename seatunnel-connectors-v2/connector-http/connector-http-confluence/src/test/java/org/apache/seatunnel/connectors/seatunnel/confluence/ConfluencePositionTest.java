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

package org.apache.seatunnel.connectors.seatunnel.confluence;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.http.Header;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Protects pre-assignment and terminal recovery, split return ordering and canonical prefix checks.
 */
class ConfluencePositionTest {
    private final ConfluenceConfig config = config();

    @Test
    void readerSnapshotsBeforeAssignmentAndRestoresTerminalState() throws Exception {
        SourceReader.Context context = mock(SourceReader.Context.class);
        Collector<SeaTunnelRow> output = collector();
        DefaultSerializer<ConfluenceSplit> serializer = new DefaultSerializer<>();
        try (ConfluenceReader reader = new ConfluenceReader(config, context)) {
            reader.addSplits(Collections.emptyList());
            reader.pollNext(output);
            ConfluenceSplit early =
                    serializer.deserialize(serializer.serialize(reader.snapshotState(1).get(0)));
            early.check(config);
            assertEquals("", early.position);
            assertNull(early.prefix);
            assertFalse(early.finished);
            ConfluenceSplit terminal =
                    new ConfluenceSplit(
                            early.binding, early.position, 1, 0, null, true, early.visited);
            reader.addSplits(
                    Collections.singletonList(
                            serializer.deserialize(serializer.serialize(terminal))));
            reader.pollNext(output);
            reader.pollNext(output);
            assertTrue(reader.snapshotState(2).get(0).finished);
            verify(context, times(1)).signalNoMoreElement();
            verifyNoInteractions(output);
        }
    }

    @Test
    void restoredEnumeratorAwaitsTheReturnedSplitOrAnExplicitEmptyRestore() throws Exception {
        SourceSplitEnumerator.Context<ConfluenceSplit> original = enumeratorContext();
        ConfluenceSplit initial = ConfluenceSplit.initial(config);
        ConfluenceEnumerator fresh = new ConfluenceEnumerator(original, initial, null);
        fresh.run();
        fresh.handleSplitRequest(0);
        verify(original, times(1)).assignSplit(0, initial);

        DefaultSerializer<ConfluenceEnumerator.State> serializer = new DefaultSerializer<>();
        ConfluenceEnumerator.State checkpoint =
                serializer.deserialize(serializer.serialize(fresh.snapshotState(1)));
        SourceSplitEnumerator.Context<ConfluenceSplit> restored = enumeratorContext();
        ConfluenceEnumerator enumerator = new ConfluenceEnumerator(restored, initial, checkpoint);
        enumerator.run();
        ConfluenceSplit terminal =
                new ConfluenceSplit(
                        initial.binding, initial.position, 1, 0, null, true, initial.visited);
        enumerator.addSplitsBack(Collections.singletonList(terminal), 0);
        enumerator.handleSplitRequest(0);
        verify(restored, times(1)).assignSplit(0, terminal);

        SourceSplitEnumerator.Context<ConfluenceSplit> unassigned = enumeratorContext();
        ConfluenceEnumerator empty = new ConfluenceEnumerator(unassigned, initial, checkpoint);
        empty.addSplitsBack(Collections.emptyList(), 0);
        empty.run();
        verify(unassigned, times(1)).assignSplit(0, initial);
    }

    @Test
    void checkpointDigestsIgnoreObjectFieldOrderAndPreserveNullAndArrays() {
        String first = record("{\"b\":{\"a\":null,\"z\":[2,1]},\"id\":1}");
        String reordered = record("{\"id\":1,\"b\":{\"z\":[2,1],\"a\":null}}");
        String changed = record("{\"id\":1,\"b\":{\"z\":[1,2],\"a\":null}}");
        assertEquals(first, reordered);
        assertFalse(first.equals(changed));
    }

    /**
     * Preserves bounded long property names, precision and UTF-8 BOM handling while malformed byte
     * sequences still fail before any row is exposed. This is payload compatibility coverage.
     */
    @Test
    void preservesLongPropertyNamesAndBomAndRejectsMalformedBytes() {
        String name = String.join("", Collections.nCopies(131072, "k"));
        String content =
                "{\"id\":\"long\",\""
                        + name
                        + "\":\"雪🦊\",\"decimal\":1.23456789101112131415,\"nothing\":null}";
        String body = "{\"results\":[" + content + "]}";
        for (String prefix : new String[] {"", "\uFEFF"}) {
            ConfluencePage page =
                    ConfluencePage.decode(
                            config,
                            (prefix + body).getBytes(StandardCharsets.UTF_8),
                            new Header[0],
                            "");
            assertEquals(1, page.rows.size());
            assertEquals(content, page.rows.get(0).value.getField(3));
            assertEquals("long", page.rows.get(0).value.getField(1));
            assertNull(page.next);
        }
        byte[] malformed = "{\"results\":[{\"id\":\"x\"}]}".getBytes(StandardCharsets.UTF_8);
        malformed[malformed.length - 5] = (byte) 0xFF;
        assertThrows(
                ConfluenceConfig.Failure.class,
                () -> ConfluencePage.decode(config, malformed, new Header[0], ""));
    }

    @Test
    void rejectsUnsafeStateAndOriginsWithoutEchoingInputs() {
        ConfluenceSplit initial = ConfluenceSplit.initial(config);
        ConfluenceSplit invalid =
                new ConfluenceSplit(
                        initial.binding, "&start=11", 0, 0, null, false, initial.visited);
        assertThrows(IllegalStateException.class, () -> invalid.check(config));
        Map<String, Object> options = options();
        options.put("site_url", "https://user:do-not-echo@example.test");
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new ConfluenceSource(ReadonlyConfig.fromMap(options)));
        assertFalse(error.getMessage().contains("do-not-echo"));
    }

    private String record(String content) {
        String body = "{\"results\":[" + content + "]}";
        return ConfluencePage.decode(
                        config, body.getBytes(StandardCharsets.UTF_8), new Header[0], "")
                .rows
                .get(0)
                .prefix;
    }

    private static ConfluenceConfig config() {
        return new ConfluenceConfig(ReadonlyConfig.fromMap(options()));
    }

    private static Map<String, Object> options() {
        Map<String, Object> options = new HashMap<>();
        options.put("site_url", "https://example.test");
        options.put("resource", "pages");
        options.put("username", "test@example.test");
        options.put("token", "fixture-token");
        return options;
    }

    @SuppressWarnings("unchecked")
    private static Collector<SeaTunnelRow> collector() {
        return mock(Collector.class);
    }

    @SuppressWarnings("unchecked")
    private static SourceSplitEnumerator.Context<ConfluenceSplit> enumeratorContext() {
        SourceSplitEnumerator.Context<ConfluenceSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        when(context.currentParallelism()).thenReturn(1);
        when(context.registeredReaders()).thenReturn(Collections.singleton(0));
        return context;
    }
}
