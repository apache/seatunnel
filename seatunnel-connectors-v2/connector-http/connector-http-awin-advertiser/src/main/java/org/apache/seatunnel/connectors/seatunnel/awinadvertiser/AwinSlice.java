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

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * One complete request window, including the correct legacy date and region arguments.
 *
 * <p>Reports use inclusive UTC dates. Transaction queries use the documented second-resolution
 * clock and leave no overlap between consecutive complete UTC days.
 */
final class AwinSlice {
    static final DateTimeFormatter SECONDS =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss", Locale.ROOT);
    final Partition partition;
    final LocalDate first;
    final LocalDate last;
    final LocalDateTime transactionEnd;

    AwinSlice(AwinSettings settings, AwinCursor cursor) {
        partition = settings.partitions.get(cursor.partitionIndex);
        first = cursor.day;
        if (first == null) {
            last = null;
            transactionEnd = null;
            return;
        }
        LocalDateTime cutoff = LocalDateTime.ofInstant(cursor.cutoff, ZoneOffset.UTC);
        LocalDate requested = first.plusDays(maxDays(settings, partition.resource) - 1L);
        last = requested.isBefore(cutoff.toLocalDate()) ? requested : cutoff.toLocalDate();
        LocalDateTime endOfDay = last.atTime(23, 59, 59);
        transactionEnd = endOfDay.isBefore(cutoff) ? endOfDay : cutoff;
    }

    String path(AwinSettings settings) {
        return partition.resource == AwinResource.ACCOUNTS
                ? partition.resource.route
                : "/advertisers/" + settings.advertiser + partition.resource.route;
    }

    /**
     * Builds only the parameters documented for the chosen legacy route.
     *
     * <p>No page, cursor or access token is added to the request query.
     */
    Map<String, String> query() {
        Map<String, String> arguments = new LinkedHashMap<>();
        switch (partition.resource) {
            case ACCOUNTS:
                arguments.put("type", "advertiser");
                break;
            case CAMPAIGN:
                reportDays(arguments);
                arguments.put("interval", "day");
                break;
            case TRANSACTIONS:
                arguments.put(
                        "startDate", LocalDateTime.of(first, LocalTime.MIDNIGHT).format(SECONDS));
                arguments.put("endDate", transactionEnd.format(SECONDS));
                arguments.put("timezone", "UTC");
                arguments.put("dateType", "transaction");
                arguments.put("showBasketProducts", "true");
                break;
            case PUBLISHER_REPORT:
                reportDays(arguments);
                arguments.put("dateType", "transaction");
                break;
            case CREATIVE:
                reportDays(arguments);
                arguments.put("dateType", "transaction");
                arguments.put("region", partition.region);
                break;
            case PUBLISHERS:
                break;
            default:
                throw new IllegalStateException("Missing Awin source resource");
        }
        return arguments;
    }

    boolean lastInPartition(Instant cutoff) {
        return last == null || last.equals(cutoff.atOffset(ZoneOffset.UTC).toLocalDate());
    }

    private void reportDays(Map<String, String> query) {
        query.put("startDate", first.toString());
        query.put("endDate", last.toString());
        query.put("timezone", "UTC");
    }

    private static int maxDays(AwinSettings settings, AwinResource resource) {
        if (resource == AwinResource.CAMPAIGN) {
            return settings.campaignDays;
        }
        return resource == AwinResource.TRANSACTIONS ? settings.transactionDays : 1;
    }

    /**
     * One independent resource or creative region and its initial lookback-adjusted date.
     *
     * <p>The plan order remains fixed for restoration and excludes unselected regional data.
     */
    static final class Partition implements Serializable {
        private static final long serialVersionUID = 1L;
        final AwinResource resource;
        final String region;
        final LocalDate first;

        Partition(AwinResource resource, String region, LocalDate first) {
            this.resource = resource;
            this.region = region;
            this.first = first;
        }
    }
}
