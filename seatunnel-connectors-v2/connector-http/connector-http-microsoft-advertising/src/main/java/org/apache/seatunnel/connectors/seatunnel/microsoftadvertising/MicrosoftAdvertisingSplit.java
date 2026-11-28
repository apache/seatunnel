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
package org.apache.seatunnel.connectors.seatunnel.microsoftadvertising;

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Serializable position of the bounded account scan.
 *
 * <p>Only a provider job identifier and file digest survive restore. Signed download URLs,
 * credentials, network objects and local temporary files stay outside the checkpoint.
 */
final class MicrosoftAdvertisingSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final String scope;
    final LocalDate asOf;
    final LocalDate start;
    final LocalDate end;
    final Instant bulkFrom;
    final List<Account> accounts;
    final Set<String> fullAccountPageDigests;
    String userId;
    boolean discovering = true;
    boolean complete;
    int predicate;
    int searchPage;
    int pagesRequested;
    int accountIndex;

    // The direct traversal is performed once per account before report and bulk work.
    int resourceIndex = -1;
    boolean campaignEmitted;
    boolean groupEmitted;
    final Position search;
    final Position campaign;
    final Position group;
    final Position ad;

    // A queued submit is checkpointed separately from a confirmed provider job.
    boolean submitPending;
    String jobId;
    Instant jobCreatedAt;
    Instant activeDelta;
    boolean jobReady;
    boolean bulkFull;
    final Position file;

    MicrosoftAdvertisingSplit(
            String scope, LocalDate asOf, LocalDate start, LocalDate end, Instant bulkFrom) {
        this.scope = scope;
        this.asOf = asOf;
        this.start = start;
        this.end = end;
        this.bulkFrom = bulkFrom;
        accounts = new ArrayList<>();
        fullAccountPageDigests = new HashSet<>();
        search = new Position();
        campaign = new Position();
        group = new Position();
        ad = new Position();
        file = new Position();
    }

    private MicrosoftAdvertisingSplit(MicrosoftAdvertisingSplit saved) {
        scope = saved.scope;
        asOf = saved.asOf;
        start = saved.start;
        end = saved.end;
        bulkFrom = saved.bulkFrom;
        accounts = new ArrayList<>(saved.accounts);
        fullAccountPageDigests = new HashSet<>(saved.fullAccountPageDigests);
        userId = saved.userId;
        discovering = saved.discovering;
        complete = saved.complete;
        predicate = saved.predicate;
        searchPage = saved.searchPage;
        pagesRequested = saved.pagesRequested;
        accountIndex = saved.accountIndex;
        resourceIndex = saved.resourceIndex;
        campaignEmitted = saved.campaignEmitted;
        groupEmitted = saved.groupEmitted;
        search = new Position(saved.search);
        campaign = new Position(saved.campaign);
        group = new Position(saved.group);
        ad = new Position(saved.ad);
        submitPending = saved.submitPending;
        jobId = saved.jobId;
        jobCreatedAt = saved.jobCreatedAt;
        jobReady = saved.jobReady;
        activeDelta = saved.activeDelta;
        bulkFull = saved.bulkFull;
        file = new Position(saved.file);
    }

    @Override
    public String splitId() {
        return "microsoft-advertising-scan";
    }

    boolean finished() {
        return complete;
    }

    /**
     * Copies all mutable position collections and cursors before the engine serializes them.
     *
     * <p>The immutable account descriptors are safe to share between position copies.
     */
    MicrosoftAdvertisingSplit copy() {
        return new MicrosoftAdvertisingSplit(this);
    }

    /**
     * Validates scope and bounds before the restored position can open the provider.
     *
     * <p>The reader subsequently verifies the authorized user and the digest of resumed data.
     */
    void validate(MicrosoftAdvertisingConfig config) {
        if (!config.scope.equals(scope)
                || asOf == null
                || start == null
                || end == null
                || start.isAfter(end)
                || end.isAfter(asOf)
                || accounts.size() > config.accounts
                || accountIndex < 0
                || accountIndex > accounts.size()
                || predicate < 0
                || predicate > Math.max(1, config.accountNames.size())
                || searchPage < 0
                || searchPage > pagesRequested
                || pagesRequested > config.accountPages
                || fullAccountPageDigests.size() > pagesRequested
                || resourceIndex < -1
                || resourceIndex >= config.resources.size()
                || jobId != null && (submitPending || jobCreatedAt == null)
                || submitPending && jobCreatedAt == null
                || jobReady && jobId == null
                || file.digest != null && !jobReady
                || userId == null && (!accounts.isEmpty() || !discovering)
                || !complete && !discovering && accountIndex >= accounts.size()
                || activeDelta != null
                        && (jobCreatedAt == null
                                || bulkFull
                                || activeDelta.isAfter(jobCreatedAt))) {
            throw new MicrosoftAdvertisingFailure("checkpoint scope or position is invalid");
        }
        if (jobId != null && !MicrosoftAdvertisingConfig.opaque(jobId, 40)
                || (jobId != null || submitPending)
                        && (discovering
                                || complete
                                || resourceIndex < 0
                                || accountIndex == accounts.size()
                                || config.resources.get(resourceIndex).kind
                                        == MicrosoftAdvertisingCatalog.Kind.ENTITY)) {
            throw new MicrosoftAdvertisingFailure("checkpoint job position is invalid");
        }
        for (Position cursor : List.of(search, campaign, group, ad, file)) {
            if (cursor.row < 0
                    || cursor.row > 0 && cursor.digest == null
                    || cursor.digest != null && !cursor.digest.matches("[a-f0-9]{64}")) {
                throw new MicrosoftAdvertisingFailure("checkpoint row or digest is invalid");
            }
        }
    }

    void clearJob() {
        submitPending = false;
        jobId = null;
        jobCreatedAt = null;
        jobReady = false;
        activeDelta = null;
        file.clear();
    }

    /**
     * Identifier and owning customer needed for scoped operations after discovery.
     *
     * <p>Neither credential values nor the full account response are retained here.
     */
    static final class Account implements Serializable {
        private static final long serialVersionUID = 1L;

        final String id;
        final String customer;

        Account(String id, String customer) {
            this.id = id;
            this.customer = customer;
        }
    }

    /**
     * Number of consumed rows in one validated response and its replay identity.
     *
     * <p>On recovery the reader reopens the response and verifies its digest before skipping rows.
     */
    static final class Position implements Serializable {
        private static final long serialVersionUID = 1L;

        int row;
        String digest;

        Position() {}

        Position(Position saved) {
            row = saved.row;
            digest = saved.digest;
        }

        void clear() {
            row = 0;
            digest = null;
        }

        void validate(String receivedDigest, int rowCount) {
            if (row > rowCount || digest != null && !digest.equals(receivedDigest)) {
                throw new MicrosoftAdvertisingFailure("checkpoint response changed before replay");
            }
        }
    }
}
