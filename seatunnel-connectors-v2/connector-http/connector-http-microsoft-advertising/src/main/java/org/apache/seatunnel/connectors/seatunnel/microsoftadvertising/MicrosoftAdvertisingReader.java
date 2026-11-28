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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingApi.Page;
import org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingApi.Poll;
import org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingApi.PollStatus;
import org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingCatalog.Kind;
import org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingCatalog.Resource;
import org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSplit.Account;
import org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSplit.Position;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Reads the account hierarchy followed by reporting and bulk jobs for each discovered account.
 *
 * <p>The provider worker owns authentication and blocking I/O. The poll thread commits emitted rows
 * with their positions under the engine checkpoint lock.
 */
final class MicrosoftAdvertisingReader
        implements SourceReader<SeaTunnelRow, MicrosoftAdvertisingSplit> {
    private final MicrosoftAdvertisingConfig config;
    private final Context context;
    private final Object checkpoint = new Object();
    private final Object lifecycle = new Object();
    private final Map<String, Account> seen = new HashMap<>();

    private final boolean campaignsSelected;
    private final boolean groupsSelected;
    private final boolean adsSelected;

    private volatile boolean closed;
    private boolean signalled;
    private boolean noMoreSplits;
    private boolean verifiedUser;
    private boolean submitStartedHere;
    private boolean replacedUrl;
    private long nextPoll;
    private String downloadUrl;
    private MicrosoftAdvertisingSplit progress;
    private Page searchPage;
    private Page campaigns;
    private Page groups;
    private Page ads;
    private MicrosoftAdvertisingFile csv;

    private MicrosoftAdvertisingHttp http;
    private MicrosoftAdvertisingApi api;
    private ExecutorService executor;
    private Future<?> pending;
    private long requestDeadline;

    MicrosoftAdvertisingReader(MicrosoftAdvertisingConfig config, Context context) {
        this.config = config;
        this.context = context;
        campaignsSelected = selected("campaigns");
        groupsSelected = selected("ad_groups");
        adsSelected = selected("ads");
    }

    /**
     * Prepares a single quiet provider worker for the active reader.
     *
     * <p>Opening a source does not connect or refresh a credential.
     */
    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && executor == null && context.getIndexOfSubtask() == 0) {
                http = new MicrosoftAdvertisingHttp(config);
                api = new MicrosoftAdvertisingApi(config, http);
                executor =
                        Executors.newSingleThreadExecutor(
                                task -> {
                                    Thread thread =
                                            new Thread(task, "microsoft-advertising-source");
                                    thread.setDaemon(true);
                                    return thread;
                                });
            }
        }
    }

    /**
     * Advances at most one provider result or sink row at a time.
     *
     * <p>Terminal splits signal completion without opening an authenticated provider request.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (checkpoint) {
            if (closed || signalled) {
                return;
            }
            if (progress == null ? noMoreSplits : progress.finished()) {
                signalled = true;
                context.signalNoMoreElement();
                return;
            }
            if (progress == null) {
                return;
            }
        }
        if (progress.submitPending && !submitStartedHere) {
            throw new MicrosoftAdvertisingFailure(
                    "checkpoint has an async submit with unknown outcome");
        }
        if (!verifiedUser) {
            if (progress.jobId != null) {
                validateJobAge(config.resources.get(progress.resourceIndex));
            }
            verifyUser(output);
        } else if (progress.discovering) {
            discover(output);
        } else {
            Account account = progress.accounts.get(progress.accountIndex);
            if (progress.resourceIndex == -1) {
                entities(output, account);
            } else {
                job(output, account, config.resources.get(progress.resourceIndex));
            }
        }
    }

    private void verifyUser(Collector<SeaTunnelRow> output) throws Exception {
        String user = get(String.class, () -> api.user(), config.requestTimeout);
        if (user == null) {
            return;
        }
        if (progress.userId != null && !progress.userId.equals(user)) {
            throw new MicrosoftAdvertisingFailure(
                    "checkpoint OAuth user differs from the authenticated user");
        }
        commit(
                output,
                () -> {
                    progress.userId = user;
                    verifiedUser = true;
                });
    }

    /**
     * Enumerates each provider predicate to its short terminal page and deduplicates account IDs.
     *
     * <p>Distinct accounts retain discovery order. Returning the same full page at a new index is
     * an error even when another predicate can legitimately rediscover those accounts.
     */
    private void discover(Collector<SeaTunnelRow> output) throws Exception {
        Position position = progress.search;
        if (searchPage == null) {
            if (position.digest == null && progress.pagesRequested == config.accountPages) {
                throw new MicrosoftAdvertisingFailure(
                        "account discovery exceeds max_account_pages");
            }
            Page received =
                    get(
                            Page.class,
                            () ->
                                    api.accounts(
                                            progress.userId,
                                            progress.predicate,
                                            progress.searchPage),
                            config.requestTimeout);
            if (received != null) {
                position.validate(received.digest, received.rows.size());
                if (position.digest == null
                        && received.rows.size() == MicrosoftAdvertisingConfig.SEARCH_SIZE
                        && progress.fullAccountPageDigests.contains(received.digest)) {
                    throw new MicrosoftAdvertisingFailure(
                            "account pagination repeated a full page");
                }
                commit(
                        output,
                        () -> {
                            if (position.digest == null) {
                                ++progress.pagesRequested;
                            }
                            position.digest = received.digest;
                            searchPage = received;
                        });
            }
            return;
        }
        if (position.row == searchPage.rows.size()) {
            commit(output, () -> finishSearchPage(position));
            return;
        }
        ObjectNode record = searchPage.rows.get(position.row);
        String id = MicrosoftAdvertisingApi.id(record.path("Id"), "account");
        String customer =
                MicrosoftAdvertisingApi.id(record.path("ParentCustomerId"), "parent customer");
        Account account = seen.get(id);
        if (account != null && !account.customer.equals(customer)) {
            throw new MicrosoftAdvertisingFailure(
                    "account changed its owning customer during discovery");
        }
        if (account == null && seen.size() == config.accounts) {
            throw new MicrosoftAdvertisingFailure("account discovery exceeds max_accounts");
        }
        commit(
                output,
                () -> {
                    if (account == null) {
                        Account discovered = new Account(id, customer);
                        if (config.emitAccounts) {
                            emit(output, "accounts", discovered, entityKey(id), record);
                        }
                        seen.put(id, discovered);
                        progress.accounts.add(discovered);
                    }
                    ++position.row;
                });
    }

    private void finishSearchPage(Position position) {
        if (searchPage.rows.size() == MicrosoftAdvertisingConfig.SEARCH_SIZE) {
            progress.fullAccountPageDigests.add(searchPage.digest);
            ++progress.searchPage;
        } else {
            progress.fullAccountPageDigests.clear();
            progress.searchPage = 0;
            ++progress.predicate;
            progress.discovering = progress.predicate < Math.max(1, config.accountNames.size());
            progress.complete = !progress.discovering && progress.accounts.isEmpty();
        }
        position.clear();
        searchPage = null;
    }

    /**
     * Walks campaigns, ad groups and ads in parent order without re-fetching unrelated parents.
     *
     * <p>Checkpointed parent pages are verified before any remaining descendant is read.
     */
    private void entities(Collector<SeaTunnelRow> output, Account account) throws Exception {
        if (!campaignsSelected && !groupsSelected && !adsSelected) {
            commit(output, this::nextJob);
        } else if (campaigns == null) {
            campaigns = page(output, progress.campaign, () -> api.campaigns(account));
        } else if (progress.campaign.row == campaigns.rows.size()) {
            commit(output, this::nextJob);
        } else {
            ObjectNode current = campaigns.rows.get(progress.campaign.row);
            String campaign = MicrosoftAdvertisingApi.id(current.path("Id"), "campaign");
            if (!progress.campaignEmitted) {
                commit(
                        output,
                        () -> {
                            if (campaignsSelected) {
                                emitEntity(
                                        output,
                                        "campaigns",
                                        account,
                                        campaign,
                                        current,
                                        null,
                                        null);
                            }
                            progress.campaignEmitted = true;
                            if (!groupsSelected && !adsSelected) {
                                nextCampaign();
                            }
                        });
            } else {
                groups(output, account, campaign);
            }
        }
    }

    private void groups(Collector<SeaTunnelRow> output, Account account, String campaign)
            throws Exception {
        if (groups == null) {
            groups = page(output, progress.group, () -> api.groups(account, campaign));
        } else if (progress.group.row == groups.rows.size()) {
            commit(output, this::nextCampaign);
        } else {
            ObjectNode current = groups.rows.get(progress.group.row);
            String group = MicrosoftAdvertisingApi.id(current.path("Id"), "ad group");
            if (!progress.groupEmitted) {
                commit(
                        output,
                        () -> {
                            if (groupsSelected) {
                                emitEntity(
                                        output,
                                        "ad_groups",
                                        account,
                                        group,
                                        current,
                                        "CampaignId",
                                        campaign);
                            }
                            progress.groupEmitted = true;
                            if (!adsSelected) {
                                nextGroup();
                            }
                        });
            } else {
                ads(output, account, group);
            }
        }
    }

    private void ads(Collector<SeaTunnelRow> output, Account account, String group)
            throws Exception {
        if (ads == null) {
            ads = page(output, progress.ad, () -> api.ads(account, group));
        } else if (progress.ad.row == ads.rows.size()) {
            commit(output, this::nextGroup);
        } else {
            ObjectNode current = ads.rows.get(progress.ad.row);
            String id = MicrosoftAdvertisingApi.id(current.path("Id"), "ad");
            commit(
                    output,
                    () -> {
                        emitEntity(output, "ads", account, id, current, "AdGroupId", group);
                        ++progress.ad.row;
                    });
        }
    }

    private Page page(Collector<SeaTunnelRow> output, Position position, Callable<Page> fetch)
            throws Exception {
        Page received = get(Page.class, fetch, config.requestTimeout);
        if (received != null) {
            position.validate(received.digest, received.rows.size());
            commit(output, () -> position.digest = received.digest);
        }
        return received;
    }

    private void nextCampaign() {
        ++progress.campaign.row;
        progress.campaignEmitted = false;
        progress.group.clear();
        progress.groupEmitted = false;
        progress.ad.clear();
        groups = null;
        ads = null;
    }

    private void nextGroup() {
        ++progress.group.row;
        progress.groupEmitted = false;
        progress.ad.clear();
        ads = null;
    }

    private void nextJob() {
        if (csv != null) {
            http.release(csv);
            csv = null;
        }
        progress.clearJob();
        progress.bulkFull = false;
        submitStartedHere = false;
        replacedUrl = false;
        nextPoll = 0;
        downloadUrl = null;
        do {
            ++progress.resourceIndex;
        } while (progress.resourceIndex < config.resources.size()
                && config.resources.get(progress.resourceIndex).kind == Kind.ENTITY);
        if (progress.resourceIndex == config.resources.size()) {
            progress.resourceIndex = -1;
            progress.campaign.clear();
            progress.group.clear();
            progress.ad.clear();
            progress.campaignEmitted = false;
            progress.groupEmitted = false;
            campaigns = null;
            groups = null;
            ads = null;
            ++progress.accountIndex;
            progress.complete = progress.accountIndex == progress.accounts.size();
        }
    }

    /**
     * Runs the create, poll and download phases of one selected account resource.
     *
     * <p>Only a recorded provider ID can be polled after restore; the signed URL is never saved.
     */
    private void job(Collector<SeaTunnelRow> output, Account account, Resource resource)
            throws Exception {
        if (csv != null) {
            commit(
                    output,
                    () -> {
                        if (progress.file.row == csv.rowCount) {
                            nextJob();
                        } else {
                            try {
                                MicrosoftAdvertisingFile.Row row = csv.next();
                                emit(output, resource.name, account, row.key, row.record);
                                ++progress.file.row;
                            } catch (MicrosoftAdvertisingFailure ex) {
                                if (!closed) {
                                    throw ex;
                                }
                            }
                        }
                    });
        } else if (progress.jobId == null) {
            create(output, account, resource);
        } else if (downloadUrl != null) {
            download(output, resource);
        } else {
            poll(output, account, resource);
        }
    }

    private void create(Collector<SeaTunnelRow> output, Account account, Resource resource)
            throws Exception {
        if (resource.kind == Kind.REPORT
                && resource.reportTime
                && resource.predefinedTime == null
                && MicrosoftAdvertisingApi.reportStart(resource, progress).isAfter(progress.end)) {
            throw new MicrosoftAdvertisingFailure(
                    resource.name + " reporting window is outside provider retention");
        }
        if (!progress.submitPending) {
            Instant now = Instant.now();
            commit(
                    output,
                    () -> {
                        progress.jobCreatedAt = now;
                        progress.activeDelta =
                                resource.kind == Kind.BULK && !progress.bulkFull
                                        ? MicrosoftAdvertisingConfig.supportedDelta(
                                                progress.bulkFrom, now)
                                        : null;
                        progress.submitPending = true;
                        submitStartedHere = true;
                    });
        }
        String job =
                get(
                        String.class,
                        () -> api.submit(resource, account, progress, progress.activeDelta),
                        config.requestTimeout);
        if (job != null) {
            commit(
                    output,
                    () -> {
                        progress.jobId = job;
                        progress.submitPending = false;
                        submitStartedHere = false;
                    });
        }
    }

    private void validateJobAge(Resource resource) {
        Duration elapsed = Duration.between(progress.jobCreatedAt, Instant.now());
        if (elapsed.compareTo(Duration.ofDays(resource.kind == Kind.BULK ? 2 : 1)) >= 0) {
            throw new MicrosoftAdvertisingFailure("saved provider job ID expired");
        }
        if (!progress.jobReady && elapsed.compareTo(Duration.ofMillis(config.pollTimeout)) >= 0) {
            throw new MicrosoftAdvertisingFailure("async polling exceeds poll_timeout_ms");
        }
    }

    private void poll(Collector<SeaTunnelRow> output, Account account, Resource resource)
            throws Exception {
        validateJobAge(resource);
        if (nextPoll - System.nanoTime() > 0) {
            TimeUnit.MILLISECONDS.sleep(5);
            return;
        }
        Poll result =
                get(
                        Poll.class,
                        () -> api.poll(resource, account, progress.jobId),
                        config.requestTimeout);
        if (result == null) {
            return;
        }
        commit(output, () -> finishPoll(result));
    }

    private void finishPoll(Poll result) {
        if (result.status == PollStatus.PENDING) {
            if (progress.jobReady) {
                throw new MicrosoftAdvertisingFailure(
                        "completed provider job returned to a pending state");
            }
            nextPoll = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.pollInterval);
        } else if (result.status == PollStatus.FULL_REQUIRED) {
            if (progress.bulkFull || progress.activeDelta == null || progress.jobReady) {
                throw new MicrosoftAdvertisingFailure(
                        "bulk download requires full sync after a full submission");
            }
            progress.clearJob();
            progress.bulkFull = true;
            nextPoll = 0;
        } else if (result.location == null) {
            if (progress.jobReady) {
                throw new MicrosoftAdvertisingFailure(
                        "completed provider job no longer returns the saved file");
            }
            nextJob();
        } else {
            progress.jobReady = true;
            downloadUrl = result.location;
        }
    }

    private void download(Collector<SeaTunnelRow> output, Resource resource) throws Exception {
        Download result =
                get(
                        Download.class,
                        () -> {
                            long expires =
                                    MicrosoftAdvertisingHttp.deadline(config.downloadTimeout);
                            return new Download(
                                    http.download(downloadUrl, resource, progress, expires));
                        },
                        config.downloadTimeout);
        if (result == null) {
            return;
        }
        commit(
                output,
                () -> {
                    if (result.file == null) {
                        if (replacedUrl) {
                            throw new MicrosoftAdvertisingFailure(
                                    "refreshed signed download URL was rejected");
                        }
                        replacedUrl = true;
                        downloadUrl = null;
                    } else {
                        progress.file.digest = result.file.digest;
                        csv = result.file;
                        downloadUrl = null;
                    }
                });
    }

    private <T> T get(Class<T> kind, Callable<T> work, int budget) throws Exception {
        Future<?> request;
        synchronized (lifecycle) {
            if (closed) {
                return null;
            }
            if (pending == null) {
                requestDeadline = MicrosoftAdvertisingHttp.deadline(budget);
                pending = executor.submit(work);
            }
            request = pending;
        }
        try {
            T result = kind.cast(request.get(5, TimeUnit.MILLISECONDS));
            synchronized (lifecycle) {
                pending = null;
                return closed ? null : result;
            }
        } catch (TimeoutException ex) {
            if (!closed && requestDeadline - System.nanoTime() <= 0) {
                synchronized (lifecycle) {
                    abortWorker();
                }
                String action =
                        progress.submitPending ? "async submit outcome may be unknown; " : "";
                throw new MicrosoftAdvertisingFailure(action + "HTTPS operation deadline exceeded");
            }
            return null;
        } catch (CancellationException ex) {
            if (closed) {
                return null;
            }
            throw new MicrosoftAdvertisingFailure("provider operation canceled");
        } catch (ExecutionException ex) {
            if (closed) {
                return null;
            }
            if (ex.getCause() instanceof MicrosoftAdvertisingFailure) {
                throw (MicrosoftAdvertisingFailure) ex.getCause();
            }
            throw new MicrosoftAdvertisingFailure("provider operation failed");
        }
    }

    private boolean selected(String name) {
        return config.resources.stream().anyMatch(resource -> resource.name.equals(name));
    }

    private String entityKey(String id) {
        return http.json.createArrayNode().add(id).toString();
    }

    private void emitEntity(
            Collector<SeaTunnelRow> output,
            String resource,
            Account account,
            String id,
            ObjectNode raw,
            String parentName,
            String parentValue) {
        ObjectNode record = raw.deepCopy();
        field(record, "AccountId", account.id);
        field(record, "CustomerId", account.customer);
        if (parentName != null) {
            field(record, parentName, parentValue);
        }
        emit(output, resource, account, entityKey(id), record);
    }

    private static void field(ObjectNode record, String name, String id) {
        JsonNode value = record.path(name);
        if (value.isMissingNode() || value.isNull()) {
            record.put(name, id);
        } else if (!id.equals(MicrosoftAdvertisingApi.id(value, "entity parent"))) {
            throw new MicrosoftAdvertisingFailure(
                    "entity response does not match its parent scope");
        }
    }

    private static void emit(
            Collector<SeaTunnelRow> output,
            String resource,
            Account account,
            String key,
            ObjectNode record) {
        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            resource, account.id, account.customer, key, record.toString()
                        });
        row.setTableId(TablePath.DEFAULT.getFullName());
        output.collect(row);
    }

    private void commit(Collector<SeaTunnelRow> output, Runnable change) {
        synchronized (output.getCheckpointLock()) {
            synchronized (checkpoint) {
                if (!closed) {
                    change.run();
                }
            }
        }
    }

    /**
     * Returns an independent position with the current page/file row and known provider job ID.
     *
     * <p>A submit that has been dispatched without a confirmed ID remains visibly ambiguous.
     */
    @Override
    public List<MicrosoftAdvertisingSplit> snapshotState(long checkpointId) {
        synchronized (checkpoint) {
            return progress == null ? List.of() : List.of(progress.copy());
        }
    }

    /**
     * Accepts one scan and reconstructs the discovery index from its previously accepted accounts.
     *
     * <p>A restored nonterminal split revalidates the OAuth user before continuing.
     */
    @Override
    public void addSplits(List<MicrosoftAdvertisingSplit> splits) {
        synchronized (checkpoint) {
            if (splits.isEmpty()) {
                return;
            }
            if (splits.size() != 1 || progress != null || context.getIndexOfSubtask() != 0) {
                throw new MicrosoftAdvertisingFailure("invalid reader split assignment");
            }
            MicrosoftAdvertisingSplit split = splits.get(0);
            split.validate(config);
            progress = split.copy();
            for (Account account : progress.accounts) {
                if (seen.put(account.id, account) != null) {
                    throw new MicrosoftAdvertisingFailure("checkpoint contains duplicate accounts");
                }
            }
        }
    }

    @Override
    public void handleNoMoreSplits() {
        synchronized (checkpoint) {
            noMoreSplits = true;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    /**
     * Cancels outstanding HTTP and closes any local report without waiting on a checkpoint lock.
     *
     * <p>Cancellation remains sticky if invoked before the reader opens.
     */
    @Override
    public void cancel() {
        synchronized (lifecycle) {
            closed = true;
            abortWorker();
        }
    }

    private void abortWorker() {
        if (http != null) {
            http.close();
        }
        if (pending != null) {
            pending.cancel(true);
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public void close() {
        cancel();
    }

    /**
     * Carries a completed download separately from an in-flight provider operation.
     *
     * <p>A missing file means an explicitly rejected signed URL eligible for one fresh poll.
     */
    private static final class Download {
        final MicrosoftAdvertisingFile file;

        Download(MicrosoftAdvertisingFile file) {
            this.file = file;
        }
    }
}
