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
package org.apache.seatunnel.connectors.seatunnel.awinadvertiser;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

/**
 * Immutable checkpoint for the selected set of dated and undated report partitions.
 *
 * <p>The UTC cutoff stays fixed across recovery. Each partly published window retains a response
 * digest and count instead of serializing report payloads.
 */
final class AwinCursor implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final String scope;
    final Instant cutoff;
    final int partitionIndex;
    final LocalDate day;
    final int completedRequests;
    final String digest;
    final int emitted;

    AwinCursor(AwinSettings settings) {
        this(
                settings.scope,
                initialCutoff(settings),
                0,
                settings.partitions.get(0).first,
                0,
                null,
                0);
    }

    private AwinCursor(
            String scope,
            Instant cutoff,
            int partitionIndex,
            LocalDate day,
            int completedRequests,
            String digest,
            int emitted) {
        this.scope = scope;
        this.cutoff = cutoff;
        this.partitionIndex = partitionIndex;
        this.day = day;
        this.completedRequests = completedRequests;
        this.digest = digest;
        this.emitted = emitted;
    }

    private static Instant initialCutoff(AwinSettings settings) {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        if (settings.end == null) {
            return now;
        }
        Instant endOfRequestedDay = settings.end.atTime(23, 59, 59).toInstant(ZoneOffset.UTC);
        return endOfRequestedDay.isBefore(now) ? endOfRequestedDay : now;
    }

    /**
     * Advances to the next day or partition only after the complete response is emitted.
     *
     * <p>A region change starts from that region's independent configured resume position.
     */
    AwinCursor advance(AwinSlice slice, AwinSettings settings) {
        int target = slice.lastInPartition(cutoff) ? partitionIndex + 1 : partitionIndex;
        LocalDate nextDay =
                target == partitionIndex
                        ? slice.last.plusDays(1)
                        : target < settings.partitions.size()
                                ? settings.partitions.get(target).first
                                : null;
        return new AwinCursor(scope, cutoff, target, nextDay, completedRequests + 1, null, 0);
    }

    AwinCursor trackResponse(String hash, int count) {
        return new AwinCursor(scope, cutoff, partitionIndex, day, completedRequests, hash, count);
    }

    @Override
    public String splitId() {
        return "awin-advertiser-scan";
    }
}
