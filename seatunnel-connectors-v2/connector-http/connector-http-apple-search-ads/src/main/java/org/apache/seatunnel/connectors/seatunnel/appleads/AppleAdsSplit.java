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
package org.apache.seatunnel.connectors.seatunnel.appleads;

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Stores one bounded Apple Ads scan and its active parent/report path.
 *
 * <p>The state contains only logical scope and accepted positions. OAuth credentials, response
 * bodies and provider error content stay outside checkpoint serialization.
 */
final class AppleAdsSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    // Mode, account, configured resource order and reporting options must match on restore.
    final String scope;

    // The original calendar and date bounds survive a restore on a later organization day.
    final Instant cutoff;
    final LocalDate first;
    final LocalDate last;

    // Parent frames remain resumable while their campaign or ad-group child is being read.
    final List<Frame> stack = new ArrayList<>();
    int resource;

    AppleAdsSplit(AppleAdsConfig config, Instant time) {
        this(config.scope, time, config.first(time), config.last(time));
    }

    private AppleAdsSplit(String scope, Instant cutoff, LocalDate first, LocalDate last) {
        this.scope = scope;
        this.cutoff = cutoff;
        this.first = first;
        this.last = last;
    }

    @Override
    public String splitId() {
        return "apple-ads-catalog";
    }

    /**
     * Makes checkpoint snapshots independent of subsequent page or window advancement.
     *
     * <p>The active path has at most campaign, ad group and terminal operation frames.
     */
    AppleAdsSplit copy() {
        AppleAdsSplit saved = new AppleAdsSplit(scope, cutoff, first, last);
        saved.resource = resource;
        for (Frame frame : stack) {
            saved.stack.add(frame.copy());
        }
        return saved;
    }

    boolean finished(AppleAdsConfig config) {
        return resource == config.resources.size();
    }

    void validate(AppleAdsConfig config) {
        if (!config.scope.equals(scope)
                || cutoff == null
                || last == null
                || resource < 0
                || resource > config.resources.size()
                || stack.size() > 3
                || finished(config) && !stack.isEmpty()) {
            throw new IllegalStateException(
                    "AppleAds checkpoint has incompatible scan options or state");
        }
    }

    /**
     * Identifies one pagination stream for a parent and optional fixed report window.
     *
     * <p>The raw provider offset counts entities; row is a separate position in daily metrics.
     */
    static final class Frame implements Serializable {
        private static final long serialVersionUID = 1L;

        final AppleAdsResource stage;
        final Long campaign;
        final Long group;
        LocalDate date;
        int offset;
        int row;
        int pageIndex;
        int total = -1;
        String pageDigest;
        String precedingDigest;

        Frame(AppleAdsResource stage, Long campaign, Long group, LocalDate date) {
            this.stage = stage;
            this.campaign = campaign;
            this.group = group;
            this.date = date;
        }

        /**
         * Records accepted page identity separately from the next raw server offset.
         *
         * <p>One exact hash comparison rejects an endpoint that repeatedly returns the same payload
         * under a changing offset.
         */
        void advance(AppleAdsApi.Page page, int maximumPages) {
            if (page.next <= offset || pageIndex + 1 >= maximumPages) {
                throw stage.fail("pagination did not advance or exceeded max_pages_per_partition");
            }
            offset = page.next;
            row = 0;
            ++pageIndex;
            total = page.total;
            pageDigest = null;
            precedingDigest = page.digest;
        }

        void advanceWindow(LocalDate next) {
            date = next;
            offset = 0;
            row = 0;
            pageIndex = 0;
            total = -1;
            pageDigest = null;
            precedingDigest = null;
        }

        private Frame copy() {
            Frame saved = new Frame(stage, campaign, group, date);
            saved.offset = offset;
            saved.row = row;
            saved.pageIndex = pageIndex;
            saved.total = total;
            saved.pageDigest = pageDigest;
            saved.precedingDigest = precedingDigest;
            return saved;
        }
    }
}
