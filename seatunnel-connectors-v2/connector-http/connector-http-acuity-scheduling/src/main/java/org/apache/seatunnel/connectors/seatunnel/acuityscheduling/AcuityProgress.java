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

package org.apache.seatunnel.connectors.seatunnel.acuityscheduling;

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.Deque;

/** Checkpointed resource and next civil-date window. The split holds no credentials or records. */
final class AcuityProgress implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final String fingerprint;
    final LocalDate end;
    int resourceIndex;
    int completedRequests;
    LocalDate nextWindowStart;
    boolean finished;
    private final Deque<Window> windows = new ArrayDeque<>();

    AcuityProgress(String fingerprint, LocalDate start, LocalDate end) {
        this.fingerprint = fingerprint;
        this.nextWindowStart = start;
        this.end = end;
    }

    AcuityProgress(AcuityProgress source) {
        this(source.fingerprint, source.nextWindowStart, source.end);
        resourceIndex = source.resourceIndex;
        completedRequests = source.completedRequests;
        finished = source.finished;
        windows.addAll(source.windows);
    }

    @Override
    public String splitId() {
        return "acuity-resource-crawl";
    }

    Window nextWindow(int days) {
        if (windows.isEmpty() && !nextWindowStart.isAfter(end)) {
            LocalDate to = nextWindowStart.plusDays(days - 1L);
            if (to.isAfter(end)) {
                to = end;
            }
            windows.addLast(new Window(nextWindowStart, to));
            nextWindowStart = to.plusDays(1);
        }
        return windows.peekFirst();
    }

    /**
     * Replaces an inclusive saturated interval by disjoint left and right windows. The worklist
     * grows only with subdivision depth, independent of the total crawl span.
     */
    void subdivide() {
        Window current = windows.removeFirst();
        LocalDate mid =
                current.start.plusDays(ChronoUnit.DAYS.between(current.start, current.end) / 2);
        windows.addFirst(new Window(mid.plusDays(1), current.end));
        windows.addFirst(new Window(current.start, mid));
    }

    void completeWindow() {
        windows.removeFirst();
    }

    void nextResource(AcuityConfig config) {
        finished = ++resourceIndex == config.resources.size();
        windows.clear();
        nextWindowStart = config.start;
    }

    /** Civil calendar dates use inclusive bounds and do not depend on the length of a DST day. */
    static final class Window implements Serializable {
        private static final long serialVersionUID = 1L;

        final LocalDate start;
        final LocalDate end;

        Window(LocalDate start, LocalDate end) {
            this.start = start;
            this.end = end;
        }

        boolean singleDay() {
            return start.equals(end);
        }
    }
}
