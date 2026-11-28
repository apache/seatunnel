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

package org.apache.seatunnel.connectors.seatunnel.appsflyer;

import org.apache.seatunnel.connectors.seatunnel.appsflyer.AppsFlyerResource.Report;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.zone.ZoneOffsetTransition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Represents one immutable, half-open report window in the engine. Aggregate queries translate it
 * to inclusive date bounds on the wire.
 */
final class AppsFlyerWindow implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final DateTimeFormatter WIRE_TIME =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss", Locale.ROOT);
    final Report report;
    final Instant start;
    final Instant end;

    AppsFlyerWindow(Report report, Instant start, Instant end) {
        this.report = report;
        this.start = start;
        this.end = end;
    }

    /**
     * Plans bounded windows from a single job time and retains them through checkpoint restore. Raw
     * availability is measured from the initial app-local date, independent of the requested end.
     */
    static List<AppsFlyerWindow> plan(AppsFlyerConfig config, Instant started) {
        Instant now = started.truncatedTo(ChronoUnit.SECONDS);
        LocalDate today = now.atZone(config.zone).toLocalDate();
        List<AppsFlyerWindow> windows = new ArrayList<>();
        for (Report report : config.reports) {
            LocalDate retainedSince = report.raw ? today.minusDays(90) : today.minusMonths(25);
            if (config.start.isBefore(retainedSince.atStartOfDay(config.zone).toInstant())) {
                throw AppsFlyerConfig.invalid(
                        "start_date is outside retention for " + report.endpoint);
            }
            Instant endBound = config.configuredEnd;
            if (endBound == null) {
                endBound =
                        report.raw ? now : today.plusDays(1).atStartOfDay(config.zone).toInstant();
            }
            Instant latest =
                    report.raw ? now : today.plusDays(1).atStartOfDay(config.zone).toInstant();
            if (endBound.isAfter(latest) || config.start.isAfter(endBound)) {
                throw AppsFlyerConfig.invalid(
                        "selected report dates extend past the available time");
            }
            Instant next = config.start;
            while (next.isBefore(endBound)) {
                LocalDate day = next.atZone(config.zone).toLocalDate();
                Instant batchEnd = day.plusDays(report.days).atStartOfDay(config.zone).toInstant();
                Instant stop = batchEnd.isBefore(endBound) ? batchEnd : endBound;
                if (report.raw
                        && (!representable(next, config.zone)
                                || !representable(stop, config.zone))) {
                    throw AppsFlyerConfig.invalid(
                            "raw request boundary is ambiguous in the configured timezone");
                }
                windows.add(new AppsFlyerWindow(report, next, stop));
                next = stop;
            }
        }
        return Collections.unmodifiableList(windows);
    }

    String from(Report requested, ZoneId zone) {
        return requested.raw
                ? WIRE_TIME.format(start.atZone(zone))
                : start.atZone(zone).toLocalDate().toString();
    }

    String to(Report requested, ZoneId zone) {
        return requested.raw
                ? WIRE_TIME.format(end.atZone(zone))
                : end.atZone(zone).toLocalDate().minusDays(1).toString();
    }

    /**
     * Divides a saturated report only at a boundary the endpoint can represent without overlap. Raw
     * dates can split at seconds and aggregate dates only at the start of a whole day.
     */
    Instant splitAt(ZoneId zone) {
        if (!report.raw) {
            LocalDate first = start.atZone(zone).toLocalDate();
            long days = ChronoUnit.DAYS.between(first, end.atZone(zone).toLocalDate());
            return days <= 1 ? null : first.plusDays(days / 2).atStartOfDay(zone).toInstant();
        }
        long seconds = ChronoUnit.SECONDS.between(start, end);
        if (seconds <= 1) {
            return null;
        }
        Instant middle = start.plusSeconds(seconds / 2);
        if (representable(middle, zone)) {
            return middle;
        }
        LocalDateTime local = LocalDateTime.ofInstant(middle, zone);
        ZoneOffsetTransition transition = zone.getRules().getTransition(local);
        long overlap = Math.abs(transition.getDuration().getSeconds());
        Instant afterFold = transition.getInstant().plusSeconds(overlap);
        if (within(afterFold)) {
            return afterFold;
        }
        Instant beforeFold = transition.getInstant().minusSeconds(overlap + 1);
        return within(beforeFold) ? beforeFold : null;
    }

    private boolean within(Instant candidate) {
        return candidate.isAfter(start) && candidate.isBefore(end);
    }

    private static boolean representable(Instant value, ZoneId zone) {
        return zone.getRules().getValidOffsets(LocalDateTime.ofInstant(value, zone)).size() == 1;
    }
}
