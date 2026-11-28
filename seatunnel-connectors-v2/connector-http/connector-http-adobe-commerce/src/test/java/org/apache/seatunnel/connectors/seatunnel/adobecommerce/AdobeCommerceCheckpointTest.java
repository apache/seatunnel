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
package org.apache.seatunnel.connectors.seatunnel.adobecommerce;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class AdobeCommerceCheckpointTest {
    private static final Instant START = Instant.parse("2024-01-01T00:00:00Z");
    private static final Instant FIRST_COORDINATOR = Instant.parse("2024-01-02T00:00:01Z");

    @Test
    @SuppressWarnings("unchecked")
    void restoresTheCoordinatorWindowBeforeTheReaderReceivedItsSplit() {
        AdobeCommerceSettings original = settings("https://tenant.example.test", "first-token");
        SourceSplitEnumerator.Context<AdobeCommerceSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        when(context.registeredReaders()).thenReturn(Collections.singleton(0));
        AdobeCommerceSource.ScanAssignment first =
                new AdobeCommerceSource.ScanAssignment(
                        context, original, original.initial(FIRST_COORDINATOR));
        AdobeCommerceSplit early = first.snapshotState(1);

        AdobeCommerceSettings rotated = settings("https://tenant.example.test", "new-token");
        rotated.verify(early);
        AdobeCommerceSource.ScanAssignment restored =
                new AdobeCommerceSource.ScanAssignment(context, rotated, early);
        restored.addSplitsBack(Collections.emptyList(), 0);
        restored.run();

        ArgumentCaptor<AdobeCommerceSplit> restoredSplit =
                ArgumentCaptor.forClass(AdobeCommerceSplit.class);
        verify(context).assignSplit(eq(0), restoredSplit.capture());
        assertEquals(FIRST_COORDINATOR, restoredSplit.getValue().end);
        assertEquals(1, restoredSplit.getValue().page);
        assertEquals("/rest/V1", rotated.prefix);
        assertThrows(
                IllegalStateException.class,
                () -> settings("https://other-tenant.example.test", "new-token").verify(early));
    }

    @Test
    @SuppressWarnings("unchecked")
    void completesRestoredTerminalReaderWithoutRequestingAPage() throws Exception {
        AdobeCommerceSettings settings = settings("https://tenant.example.test", "fixture-token");
        AdobeCommerceSplit terminal = settings.initial(FIRST_COORDINATOR);
        terminal.resource = settings.resources.size();

        SourceReader.Context context = mock(SourceReader.Context.class);
        Collector<SeaTunnelRow> collector = mock(Collector.class);
        try (AdobeCommerceReader reader = new AdobeCommerceReader(settings, context)) {
            reader.open();
            reader.addSplits(Collections.singletonList(terminal));
            reader.pollNext(collector);
            reader.pollNext(collector);
            assertEquals(terminal.resource, reader.snapshotState(2).get(0).resource);
            verify(context).signalNoMoreElement();
            verifyNoInteractions(collector);
        }
    }

    private static AdobeCommerceSettings settings(String base, String token) {
        return new AdobeCommerceSettings(
                ReadonlyConfig.fromMap(
                        Map.of("base_url", base, "token", token, "start_time", START.toString())));
    }
}
