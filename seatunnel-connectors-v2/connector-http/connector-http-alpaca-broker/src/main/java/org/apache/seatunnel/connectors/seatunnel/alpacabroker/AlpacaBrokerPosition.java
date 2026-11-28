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

package org.apache.seatunnel.connectors.seatunnel.alpacabroker;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSettings.Resource.ACCOUNTS_ACTIVITIES;

/**
 * Position after each complete request, including a frozen upper bound and any pending parents.
 *
 * <p>A state value survives reader and enumerator serialization without opening a network
 * connection.
 */
final class AlpacaBrokerPosition implements SourceSplit {
    private static final long serialVersionUID = 1L;
    static final String ID = "alpaca-broker";

    final String stateKey;
    final Instant fixedTime;
    final LocalDate fixedDate;
    final int requests;
    final String cursor;
    final Set<String> activityCursors;
    final List<String> pendingParents;
    final int parentOffset;
    final boolean parentDone;
    final boolean done;

    private AlpacaBrokerPosition(
            String stateKey,
            Instant fixedTime,
            LocalDate fixedDate,
            int requests,
            String cursor,
            Set<String> activityCursors,
            List<String> pendingParents,
            int parentOffset,
            boolean parentDone,
            boolean done) {
        this.stateKey = stateKey;
        this.fixedTime = fixedTime;
        this.fixedDate = fixedDate;
        this.requests = requests;
        this.cursor = cursor;
        this.activityCursors = activityCursors;
        this.pendingParents = pendingParents;
        this.parentOffset = parentOffset;
        this.parentDone = parentDone;
        this.done = done;
    }

    static AlpacaBrokerPosition initial(AlpacaBrokerSettings settings) {
        Instant now = Instant.now();
        Instant fixed =
                settings.endTime != null && settings.endTime.isBefore(now) ? settings.endTime : now;
        boolean activities = settings.resource == ACCOUNTS_ACTIVITIES;
        if (activities) {
            fixed = fixed.truncatedTo(ChronoUnit.SECONDS);
        }
        LocalDate day =
                settings.endDate == null
                        ? now.atOffset(ZoneOffset.UTC).toLocalDate()
                        : settings.endDate;
        boolean empty =
                (settings.startTime != null
                                && (activities
                                        ? !settings.startTime.isBefore(fixed)
                                        : settings.startTime.isAfter(fixed)))
                        || (settings.startDate != null && settings.startDate.isAfter(day));
        return new AlpacaBrokerPosition(
                settings.stateKey, fixed, day, 0, null, Set.of(), List.of(), 0, false, empty);
    }

    AlpacaBrokerPosition afterActivities(String next) {
        Set<String> seen = new HashSet<>(activityCursors);
        seen.add(AlpacaBrokerSettings.digest(next));
        return new AlpacaBrokerPosition(
                stateKey,
                fixedTime,
                fixedDate,
                requests + 1,
                next,
                Set.copyOf(seen),
                List.of(),
                0,
                false,
                false);
    }

    AlpacaBrokerPosition afterAccounts(String next) {
        return new AlpacaBrokerPosition(
                stateKey,
                fixedTime,
                fixedDate,
                requests + 1,
                next,
                activityCursors,
                List.of(),
                0,
                false,
                next == null);
    }

    AlpacaBrokerPosition withParents(List<String> parents, String next) {
        return new AlpacaBrokerPosition(
                stateKey,
                fixedTime,
                fixedDate,
                requests + 1,
                next,
                activityCursors,
                List.copyOf(parents),
                0,
                next == null,
                parents.isEmpty() && next == null);
    }

    AlpacaBrokerPosition afterDocument() {
        int offset = parentOffset + 1;
        boolean exhausted = offset == pendingParents.size();
        return new AlpacaBrokerPosition(
                stateKey,
                fixedTime,
                fixedDate,
                requests + 1,
                cursor,
                activityCursors,
                exhausted ? List.of() : pendingParents,
                exhausted ? 0 : offset,
                parentDone,
                parentDone && exhausted);
    }

    AlpacaBrokerPosition finish() {
        return new AlpacaBrokerPosition(
                stateKey,
                fixedTime,
                fixedDate,
                requests + 1,
                null,
                Set.of(),
                List.of(),
                0,
                true,
                true);
    }

    @Override
    public String splitId() {
        return ID;
    }
}
