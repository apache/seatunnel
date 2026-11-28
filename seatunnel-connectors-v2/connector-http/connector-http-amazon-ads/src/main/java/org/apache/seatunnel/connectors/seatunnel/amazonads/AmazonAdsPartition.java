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
package org.apache.seatunnel.connectors.seatunnel.amazonads;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Separates each profile's authorized scope and attribution calendar from global reporting dates.
 *
 * <p>Provider profile dates are evaluated against the frozen coordinator clock on both start and
 * restore.
 */
final class AmazonAdsPartition {
    final String profile;
    final JsonNode source;
    final boolean selected;

    AmazonAdsPartition(AmazonAdsConfig config, AmazonAdsRecord row) {
        if (!row.id.matches("[0-9]{1,40}")) {
            throw AmazonAdsResource.PROFILES.fail("profileId is not a canonical numeric scope");
        }
        profile = row.id;
        source = row.value;
        selected = config.selectedProfile(profile, source);
    }

    Window attribution(AmazonAdsConfig config, AmazonAdsResource resource, Instant cutoff) {
        String timezone = source.path("timezone").asText("");
        LocalDate today;
        try {
            today = cutoff.atZone(ZoneId.of(timezone)).toLocalDate();
        } catch (DateTimeException e) {
            throw resource.fail("profile has no valid attribution timezone");
        }
        LocalDate upper = config.end == null || today.isBefore(config.end) ? today : config.end;
        LocalDate floor = today.minusDays(60);
        LocalDate lower = config.start == null ? upper : later(config.start, floor);
        if (lower.isAfter(upper)) {
            throw resource.fail("selection is outside the supported attribution dates");
        }
        return new Window(lower, upper);
    }

    static Window firstReport(
            AmazonAdsConfig config,
            AmazonAdsResource resource,
            Instant cutoff,
            LocalDate resumeFrom) {
        LocalDate today = cutoff.atOffset(ZoneOffset.UTC).toLocalDate();
        LocalDate upper = config.end == null || today.isBefore(config.end) ? today : config.end;
        LocalDate floor = today.minusDays(60);
        LocalDate first =
                config.start == null ? upper.minusDays(1L + config.overlap) : config.start;
        LocalDate from = resumeFrom == null ? later(first, floor) : resumeFrom;
        if (resumeFrom != null && resumeFrom.isAfter(upper)) {
            return null;
        }
        if (from.isAfter(upper)) {
            throw resource.fail("selection is outside the supported reporting dates");
        }
        LocalDate windowEnd = from.plusDays(config.reportDays - 1L);
        return new Window(from, windowEnd.isBefore(upper) ? windowEnd : upper);
    }

    private static LocalDate later(LocalDate a, LocalDate b) {
        return a.isAfter(b) ? a : b;
    }

    /**
     * A pair of inclusive dates sent to the Ads report APIs.
     *
     * <p>The end date is included in the partition and the next window starts on the following day.
     */
    static final class Window {
        final LocalDate from;
        final LocalDate to;

        Window(LocalDate from, LocalDate to) {
            this.from = from;
            this.to = to;
        }

        String compactFrom() {
            return from.format(DateTimeFormatter.BASIC_ISO_DATE);
        }

        String compactTo() {
            return to.format(DateTimeFormatter.BASIC_ISO_DATE);
        }
    }
}
