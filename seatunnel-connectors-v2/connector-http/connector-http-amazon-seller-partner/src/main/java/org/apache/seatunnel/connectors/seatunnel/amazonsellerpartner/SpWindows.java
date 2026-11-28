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

package org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;

import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpPosition.Phase.API;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpPosition.Phase.DONE;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpPosition.Phase.GENERATED_LIST;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpPosition.Phase.LOOKUP;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpResource.Api.FBA_ITEMS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpResource.Api.FBA_SHIPMENTS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpResource.Api.FINANCE_EVENTS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpResource.Api.FINANCE_GROUPS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpResource.Api.ORDERS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpResource.Api.ORDER_ITEMS;

/**
 * Resolves provider-specific UTC windows once for a run.
 *
 * <p>Each internal slice has an exclusive upper edge and uses the original cutoff on restore.
 */
final class SpWindows {
    private SpWindows() {}

    static SpPosition initial(SpSettings settings, Instant clock) {
        Instant now = clock.truncatedTo(ChronoUnit.SECONDS);
        SpResource resource = settings.resource;
        if (resource.api == FBA_SHIPMENTS || resource.api == FBA_ITEMS) {
            Instant end = minimum(settings.inboundEnd, now);
            Instant start =
                    settings.inboundFixed
                            ? settings.inboundStart
                            : end.minus(settings.inboundDays, ChronoUnit.DAYS);
            return position(settings, start, end, API);
        }

        if (resource.window == SpResource.Window.SNAPSHOT) {
            return new SpPosition(settings.fingerprint, now, now, now, LOOKUP);
        }

        Instant edge =
                resource.api == ORDERS
                                || resource.api == ORDER_ITEMS
                                || resource.api == FINANCE_EVENTS
                                || resource.api == FINANCE_GROUPS
                        ? now.minusSeconds(120)
                        : now;
        if (resource.isVendorDailyAnalytics() && settings.end == null) {
            // Amazon publishes daily vendor analytics after the period closes.
            edge = now.minus(72, ChronoUnit.HOURS);
        }
        Instant end = minimum(settings.end, edge);
        int history = resource.api == SpResource.Api.VENDOR_SHIPPING ? 90 : 730;
        if (resource.window == SpResource.Window.GENERATED) {
            history = resource.maxDays;
        }
        Instant earliest =
                resource.api == SpResource.Api.VENDOR_ORDERS
                        ? now.atOffset(ZoneOffset.UTC).minusMonths(6).toInstant()
                        : now.minus(history, ChronoUnit.DAYS);
        Instant start = settings.start == null ? earliest : settings.start;
        if (resource.isReport() && settings.start != null) {
            start = start.minus(settings.reportLookback, ChronoUnit.HOURS);
        }
        if (start.isBefore(earliest)) {
            start = earliest;
        }

        if (resource.window == SpResource.Window.PRIOR_WEEK) {
            LocalDate ending = date(end).with(TemporalAdjusters.previousOrSame(DayOfWeek.SUNDAY));
            return position(settings, midnight(ending.minusDays(7)), midnight(ending), LOOKUP);
        }
        if (resource.window == SpResource.Window.DAY
                || resource.window == SpResource.Window.MONTH) {
            LocalDate lo = date(start);
            LocalDate hi = date(end);
            if (resource.window == SpResource.Window.MONTH) {
                lo = lo.withDayOfMonth(1);
                if (midnight(lo).isBefore(start)) {
                    lo = lo.plusMonths(1);
                }
                hi = hi.withDayOfMonth(1);
            } else if (midnight(lo).isBefore(start)) {
                lo = lo.plusDays(1);
            }
            start = midnight(lo);
            end = midnight(hi);
        }
        SpPosition.Phase mode = resource.isReport() ? LOOKUP : API;
        if (resource.window == SpResource.Window.GENERATED) {
            mode = GENERATED_LIST;
        }
        return position(settings, start, end, mode);
    }

    private static SpPosition position(
            SpSettings settings, Instant from, Instant cutoff, SpPosition.Phase phase) {
        boolean empty = !from.isBefore(cutoff);
        Instant until = empty ? cutoff : nextCutoff(settings, from, cutoff);
        return new SpPosition(settings.fingerprint, cutoff, from, until, empty ? DONE : phase);
    }

    static void advance(SpSettings settings, SpPosition state) {
        state.resetPages();
        state.resetReport();
        if (!state.until.isBefore(state.cutoff)
                || settings.resource.window == SpResource.Window.SNAPSHOT) {
            state.phase = DONE;
            return;
        }
        state.from = state.until;
        state.until = nextCutoff(settings, state.from, state.cutoff);
        state.phase = settings.resource.isReport() ? LOOKUP : API;
    }

    private static Instant nextCutoff(SpSettings settings, Instant from, Instant cutoff) {
        SpResource resource = settings.resource;
        if (resource.api == FINANCE_GROUPS || resource.api == FINANCE_EVENTS) {
            return minimum(from.plus(settings.financialStep), cutoff);
        }
        if (resource.api == FBA_SHIPMENTS
                || resource.api == FBA_ITEMS
                || resource.window == SpResource.Window.GENERATED
                || resource.window == SpResource.Window.PRIOR_WEEK) {
            return cutoff;
        }
        if (resource.window == SpResource.Window.MONTH) {
            return minimum(midnight(date(from).plusMonths(1)), cutoff);
        }
        int days = settings.period;
        if (resource.api == ORDERS || resource.api == ORDER_ITEMS) {
            days = Math.min(days, 30);
        } else if (resource.api != null) {
            days = Math.min(days, 7);
        } else {
            days = Math.min(days, resource.maxDays);
        }
        Instant next = from.plus(Duration.ofDays(days));
        if (resource.api != null
                && resource.api.account == SpResource.Account.VENDOR
                && days == 7) {
            // Vendor lower bounds are strict. Leave room for the preceding nanosecond.
            next = next.minusNanos(1);
        }
        return minimum(next, cutoff);
    }

    static Instant inclusiveUpper(SpPosition state) {
        return state.until.minusSeconds(1);
    }

    static LocalDate date(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC).toLocalDate();
    }

    private static Instant midnight(LocalDate date) {
        return date.atStartOfDay().toInstant(ZoneOffset.UTC);
    }

    private static Instant minimum(Instant value, Instant fallback) {
        return value == null || fallback.isBefore(value) ? fallback : value;
    }
}
