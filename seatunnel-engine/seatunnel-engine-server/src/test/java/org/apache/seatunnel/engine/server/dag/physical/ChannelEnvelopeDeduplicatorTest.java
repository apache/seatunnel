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

package org.apache.seatunnel.engine.server.dag.physical;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests channel sequence deduplication and digest conflict detection. */
public class ChannelEnvelopeDeduplicatorTest {

    @Test
    void testDuplicateEnvelopeWithSameDigestIsIgnored() {
        ChannelEnvelopeDeduplicator deduplicator = new ChannelEnvelopeDeduplicator();
        ChannelEnvelope envelope =
                new ChannelEnvelope(
                        attemptId(),
                        1L,
                        ChannelEnvelope.EnvelopeCategory.DATA,
                        new byte[] {1, 2, 3});

        Assertions.assertTrue(deduplicator.accept(envelope));
        Assertions.assertFalse(deduplicator.accept(envelope));
    }

    @Test
    void testSameIdentityWithDifferentDigestFailsFast() {
        ChannelEnvelopeDeduplicator deduplicator = new ChannelEnvelopeDeduplicator();

        Assertions.assertTrue(
                deduplicator.accept(
                        new ChannelEnvelope(
                                attemptId(),
                                1L,
                                ChannelEnvelope.EnvelopeCategory.DATA,
                                new byte[] {1, 2, 3})));
        Assertions.assertThrows(
                IllegalStateException.class,
                () ->
                        deduplicator.accept(
                                new ChannelEnvelope(
                                        attemptId(),
                                        1L,
                                        ChannelEnvelope.EnvelopeCategory.DATA,
                                        new byte[] {1, 2, 4})));
    }

    @Test
    void testNewIdentityFailsFastWhenDedupLimitIsExceeded() {
        ChannelEnvelopeDeduplicator deduplicator = new ChannelEnvelopeDeduplicator(1);
        ChannelEnvelope envelope =
                new ChannelEnvelope(
                        attemptId(),
                        1L,
                        ChannelEnvelope.EnvelopeCategory.DATA,
                        new byte[] {1, 2, 3});

        Assertions.assertTrue(deduplicator.accept(envelope));
        Assertions.assertFalse(deduplicator.accept(envelope));
        Assertions.assertThrows(
                IllegalStateException.class,
                () ->
                        deduplicator.accept(
                                new ChannelEnvelope(
                                        attemptId(),
                                        2L,
                                        ChannelEnvelope.EnvelopeCategory.DATA,
                                        new byte[] {4, 5, 6})));
    }

    private static ChannelAttemptId attemptId() {
        return new ChannelAttemptId(
                1L, new LogicalChannelKey("job", "lookup", "source", 2L, 0, 0, 0), 3L, 4L, 5L);
    }
}
