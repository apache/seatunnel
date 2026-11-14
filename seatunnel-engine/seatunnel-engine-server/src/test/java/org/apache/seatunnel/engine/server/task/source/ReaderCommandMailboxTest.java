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

package org.apache.seatunnel.engine.server.task.source;

import org.apache.seatunnel.engine.common.config.server.ManagedSourceRuntimeConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ReaderCommandMailboxTest {

    @Test
    void shouldAdmitStrictSequenceAndPreserveReservedControlCapacity() {
        ManagedSourceRuntimeConfig config = mailboxConfig();
        ManagedSourceMemoryBudget workerBudget = new ManagedSourceMemoryBudget(8192L);
        ReaderCommandMailbox mailbox = new ReaderCommandMailbox(config, workerBudget, 1L);

        SourceCommandEnvelope first = command(1L, SourceCommandKind.ASSIGN_SPLITS);
        Assertions.assertEquals(
                SourceCommandAdmissionStatus.ACCEPTED, mailbox.offer(first).getStatus());
        Assertions.assertEquals(
                SourceCommandAdmissionStatus.DUPLICATE, mailbox.offer(first).getStatus());

        SourceCommandAdmissionAck gap = mailbox.offer(command(3L, SourceCommandKind.ASSIGN_SPLITS));
        Assertions.assertEquals(SourceCommandAdmissionStatus.RETRY_LATER, gap.getStatus());
        Assertions.assertEquals(2L, gap.getExpectedSequence());

        Assertions.assertEquals(
                SourceCommandAdmissionStatus.ACCEPTED,
                mailbox.offer(command(2L, SourceCommandKind.ASSIGN_SPLITS)).getStatus());
        Assertions.assertEquals(
                SourceCommandAdmissionStatus.ACCEPTED,
                mailbox.offer(command(3L, SourceCommandKind.BARRIER)).getStatus());
        Assertions.assertEquals(1, mailbox.reservedCommands());
        Assertions.assertEquals(
                SourceCommandAdmissionStatus.TERMINAL_REJECTED,
                mailbox.offer(command(4L, SourceCommandKind.CANCEL)).getStatus());

        Assertions.assertEquals(1L, mailbox.pollNext().getSenderSequence());
        Assertions.assertEquals(2L, mailbox.pollNext().getSenderSequence());
        Assertions.assertEquals(3L, mailbox.pollNext().getSenderSequence());
        Assertions.assertNull(mailbox.pollNext());
        Assertions.assertEquals(0L, workerBudget.getUsedBytes());
    }

    @Test
    void shouldRejectDifferentCommandForOccupiedSequenceAndReleaseOnClose() {
        ManagedSourceRuntimeConfig config = mailboxConfig();
        ManagedSourceMemoryBudget workerBudget = new ManagedSourceMemoryBudget(8192L);
        ReaderCommandMailbox mailbox = new ReaderCommandMailbox(config, workerBudget, 1L);

        Assertions.assertEquals(
                SourceCommandAdmissionStatus.ACCEPTED,
                mailbox.offer(command(1L, SourceCommandKind.ASSIGN_SPLITS)).getStatus());
        Assertions.assertEquals(
                SourceCommandAdmissionStatus.INVALID_PAYLOAD,
                mailbox.offer(command(1L, SourceCommandKind.ASSIGN_SPLITS)).getStatus());
        Assertions.assertTrue(workerBudget.getUsedBytes() > 0L);

        mailbox.close();

        Assertions.assertEquals(0L, workerBudget.getUsedBytes());
        Assertions.assertEquals(
                SourceCommandAdmissionStatus.TERMINAL_REJECTED,
                mailbox.offer(command(2L, SourceCommandKind.BARRIER)).getStatus());
    }

    private static ManagedSourceRuntimeConfig mailboxConfig() {
        ManagedSourceRuntimeConfig config = new ManagedSourceRuntimeConfig();
        config.setReaderMailboxMaxCommands(3);
        config.setReaderMailboxMaxBytes(4096L);
        config.setReaderReservedControlCommands(1);
        config.setReaderReservedControlBytes(512L);
        config.setMaxCommandPayloadBytes(512);
        // The reserved control band must cover coordinator async concurrency; this fixture models
        // a single-slot reserved band, so pin concurrency to 1 instead of the default of 4.
        config.setCoordinatorAsyncMaxConcurrency(1);
        config.validate();
        return config;
    }

    private static SourceCommandEnvelope command(long sequence, SourceCommandKind kind) {
        return SourceCommandEnvelope.create(
                1L,
                2L,
                "epoch",
                "sender-attempt",
                "target-attempt",
                sequence,
                kind,
                SourceCommandDurability.CHECKPOINT_COUPLED,
                SourceCommandCodec.PAYLOAD_VERSION,
                SourceCommandCodec.EMPTY_CODEC,
                "",
                0,
                1,
                new byte[0]);
    }
}
