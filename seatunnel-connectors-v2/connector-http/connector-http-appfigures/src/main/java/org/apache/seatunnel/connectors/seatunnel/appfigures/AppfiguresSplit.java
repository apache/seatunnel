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

package org.apache.seatunnel.connectors.seatunnel.appfigures;

import org.apache.seatunnel.api.source.SourceSplit;

import java.time.LocalDate;
import java.util.List;
import java.util.Set;

/**
 * Recoverable position of one Appfigures resource or its store-product discovery.
 *
 * <p>A partially emitted response retains its digest and offset. Paged collections retain their
 * initial total and hashed identities to detect obvious drift between pages.
 */
final class AppfiguresSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    final String fingerprint;
    final LocalDate reportStart;
    final LocalDate reportEnd;
    boolean discovering;
    boolean done;
    int page = 1;
    int expectedPages = -1;
    int expectedCount = -1;
    int wirePageSize;
    Set<String> identities = Set.of();
    List<String> products = List.of();
    String digest;
    int offset;

    AppfiguresSplit(AppfiguresSettings settings, LocalDate today) {
        fingerprint = settings.fingerprint;
        discovering = settings.resource.report() && settings.store != null;
        boolean defaultDates = settings.reportStart == null && settings.groups.contains("date");
        reportStart = defaultDates ? today.minusDays(30) : settings.reportStart;
        reportEnd = defaultDates ? today.minusDays(1) : settings.reportEnd;
    }

    private AppfiguresSplit(AppfiguresSplit value) {
        fingerprint = value.fingerprint;
        reportStart = value.reportStart;
        reportEnd = value.reportEnd;
        discovering = value.discovering;
        done = value.done;
        page = value.page;
        expectedPages = value.expectedPages;
        expectedCount = value.expectedCount;
        wirePageSize = value.wirePageSize;
        identities = value.identities;
        products = value.products;
        digest = value.digest;
        offset = value.offset;
    }

    AppfiguresSplit copy() {
        return new AppfiguresSplit(this);
    }

    void reportPhase() {
        discovering = false;
        page = 1;
        expectedPages = -1;
        expectedCount = -1;
        wirePageSize = 0;
        identities = Set.of();
        done = products.isEmpty();
    }

    @Override
    public String splitId() {
        return "appfigures";
    }
}
