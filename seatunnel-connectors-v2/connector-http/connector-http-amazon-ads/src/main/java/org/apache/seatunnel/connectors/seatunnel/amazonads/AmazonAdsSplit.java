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

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

/**
 * Stores one bounded scan's frozen clock and recovery positions. It contains no credentials, signed
 * URLs, or private spool file names.
 */
final class AmazonAdsSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    final String scope;
    final Instant cutoff;
    int profile;
    boolean profileEmitted;
    String profileDigest;
    int resource;
    Cursor page = new Cursor();
    int group;
    boolean groupEmitted;
    int recommendation;
    Cursor child = new Cursor();
    String groupDigest;

    // Report ID and local spool identity are committed separately from the provider URL.
    LocalDate reportFrom;
    String reportId;
    String reportFingerprint;
    long reportStarted;
    long reportOffset;
    int reportRows;
    String reportDigest;
    boolean finished;

    AmazonAdsSplit(String scope, Instant cutoff) {
        this.scope = scope;
        this.cutoff = cutoff;
    }

    @Override
    public String splitId() {
        return "amazonads-catalog";
    }

    AmazonAdsSplit copy() {
        AmazonAdsSplit out = new AmazonAdsSplit(scope, cutoff);
        out.profile = profile;
        out.profileEmitted = profileEmitted;
        out.profileDigest = profileDigest;
        out.resource = resource;
        out.page = page.copy();
        out.group = group;
        out.groupEmitted = groupEmitted;
        out.recommendation = recommendation;
        out.child = child.copy();
        out.groupDigest = groupDigest;
        out.reportFrom = reportFrom;
        out.reportId = reportId;
        out.reportFingerprint = reportFingerprint;
        out.reportStarted = reportStarted;
        out.reportOffset = reportOffset;
        out.reportRows = reportRows;
        out.reportDigest = reportDigest;
        out.finished = finished;
        return out;
    }

    void nextProfile() {
        profile++;
        profileEmitted = false;
        resource = 0;
        clearResource();
    }

    void nextResource() {
        resource++;
        clearResource();
    }

    void clearResource() {
        page = new Cursor();
        clearAdGroupPage();
        reportFrom = null;
        clearReport();
    }

    void clearAdGroupPage() {
        group = 0;
        groupDigest = null;
        clearAdGroup();
    }

    void clearAdGroup() {
        groupEmitted = false;
        recommendation = 0;
        child = new Cursor();
    }

    void clearReport() {
        reportId = null;
        reportFingerprint = null;
        reportStarted = 0;
        reportOffset = 0;
        reportRows = 0;
        reportDigest = null;
    }

    void validate(AmazonAdsConfig config) {
        if (!config.scope.equals(scope) || cutoff == null) {
            throw new IllegalArgumentException(
                    "AmazonAds checkpoint does not match the configured source scope");
        }
        if (profile < 0
                || profile > config.maxProfiles
                || resource < 0
                || resource > config.profileResources.size()
                || group < 0
                || group > config.pageSize
                || recommendation < 0
                || recommendation > config.adGroupResources.size()
                || page == null
                || child == null
                || (profileDigest == null
                        ? profile != 0 || profileEmitted || finished
                        : !AmazonAdsRecord.isHash(profileDigest))
                || !profileEmitted && resource != 0) {
            throw invalid();
        }
        page.validate(config);
        child.validate(config);
        AmazonAdsResource active =
                resource < config.profileResources.size()
                        ? config.profileResources.get(resource)
                        : null;
        boolean nested =
                active == AmazonAdsResource.SPONSORED_PRODUCT_AD_GROUPS
                        && !config.adGroupResources.isEmpty();
        if ((!nested
                        && (group != 0
                                || groupEmitted
                                || recommendation != 0
                                || groupDigest != null
                                || !child.initial()))
                || (nested
                        && (groupDigest != null && !AmazonAdsRecord.isHash(groupDigest)
                                || groupDigest == null && (group != 0 || groupEmitted)
                                || !groupEmitted && (recommendation != 0 || !child.initial())
                                || page.row != 0))) {
            throw invalid();
        }
        if (active == null || active.kind != AmazonAdsResource.Kind.REPORT) {
            if (reportFrom != null
                    || reportId != null
                    || reportStarted != 0
                    || reportFingerprint != null
                    || reportRows != 0
                    || reportOffset != 0
                    || reportDigest != null) {
                throw invalid();
            }
        } else {
            if (!page.initial()
                    || reportRows < 0
                    || reportRows > config.maxRows
                    || reportOffset < 0
                    || reportOffset > config.maxReport
                    || (reportRows == 0) != (reportOffset == 0)
                    || (reportRows == 0
                            ? reportDigest != null
                            : !AmazonAdsRecord.isHash(reportDigest))
                    || (reportId == null
                            ? reportStarted != 0 || reportFingerprint != null || reportRows != 0
                            : reportFrom == null
                                    || reportStarted <= 0
                                    || !AmazonAdsReports.validId(reportId)
                                    || !AmazonAdsRecord.isHash(reportFingerprint))) {
                throw invalid();
            }
        }
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException("AmazonAds checkpoint contains an invalid position");
    }

    /**
     * Tracks provider continuation, row offset and integrity without retaining a response payload.
     *
     * <p>A worker refetches its saved page and verifies its digest before skipping emitted rows.
     */
    static final class Cursor implements Serializable {
        private static final long serialVersionUID = 1L;
        int page;
        long offset;
        int row;
        String token;
        long total = -1;
        String digest;
        String previousKeys;
        Set<String> priorTokens;

        Cursor copy() {
            Cursor out = new Cursor();
            out.page = page;
            out.offset = offset;
            out.row = row;
            out.token = token;
            out.total = total;
            out.digest = digest;
            out.previousKeys = previousKeys;
            out.priorTokens = priorTokens == null ? null : new HashSet<>(priorTokens);
            return out;
        }

        boolean saw(String next) {
            return priorTokens != null && priorTokens.contains(AmazonAdsRecord.hash(next));
        }

        void advance(AmazonAdsDirect.Page result) {
            page++;
            offset += result.rows.size();
            row = 0;
            token = result.next;
            total = result.total;
            digest = null;
            previousKeys = result.keys;
            if (token != null) {
                if (priorTokens == null) {
                    priorTokens = new HashSet<>();
                }
                priorTokens.add(AmazonAdsRecord.hash(token));
            }
        }

        boolean initial() {
            return page == 0 && row == 0;
        }

        void validate(AmazonAdsConfig config) {
            if (page < 0
                    || page >= config.maxPages
                    || offset < 0
                    || offset > 10000L * config.maxPages
                    || row < 0
                    || row > 10000
                    || (row == 0 ? digest != null : !AmazonAdsRecord.isHash(digest))
                    || total < -1
                    || token != null
                            && (token.length() > 8192
                                    || token.isBlank()
                                    || token.codePoints().anyMatch(Character::isISOControl)
                                    || priorTokens == null
                                    || !saw(token))
                    || priorTokens != null && priorTokens.size() > page
                    || page == 0 && (token != null || priorTokens != null || offset != 0)
                    || previousKeys != null && !AmazonAdsRecord.isHash(previousKeys)) {
                throw invalid();
            }
        }
    }
}
