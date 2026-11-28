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

package org.apache.seatunnel.connectors.seatunnel.amazonkinesis;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/**
 * Owns a fair one-page read loop and a checkpointed graph of shard positions. Worker I/O stays
 * outside checkpoint collection; only collected records advance durable sequences.
 */
final class KinesisReader implements SourceReader<SeaTunnelRow, KinesisSplit> {
    private static final int MAX_SHARDS = 8192;
    private static final int MAX_DISCOVERY_PAGES = 128;
    private static final int ROWS_PER_POLL = 64;

    private final KinesisConnection.Settings connectionSettings;
    private final AmazonKinesisSource.ReadSettings options;
    private final Context context;
    private final boolean streaming;
    private final boolean checkpointing;
    private final String scope;
    private final Object state = new Object();
    private final Object lifecycle = new Object();
    // State-lock owned; provider tokens are transient and reacquired from the split after restore.
    private final Map<String, Cursor> cursors = new HashMap<>();
    // First snapshot containing a completed parent; only a later successful notification releases
    // it.
    private final Map<String, Long> firstFinishedCheckpoint = new HashMap<>();
    // State-lock owned completion gate for descendants, initialized from restored durable state.
    private final Set<String> durableParents = new HashSet<>();

    // These are worker lifecycle fields. Cancel never needs the engine checkpoint lock.
    private volatile boolean closed;
    private KinesisConnection connection;
    private ExecutorService worker;

    // Logical state and request ownership are serialized by the reader's state lock.
    private KinesisSplit split;
    private boolean assignedNoMore;
    private boolean signaled;
    private boolean verified;
    // Per-attempt catch-up bound, reset on assignment/restore rather than serialized as wall time.
    private long batchDeadline;
    private long nextDiscovery;
    // Monotonic absolute budget for the published discovery/read Future, not an idle timeout.
    private long requestDeadline;
    private String lastShard;
    private Future<Result> request;
    // Engine-thread page cursor; snapshot persists only the sequence after successful collection.
    private Page page;
    private int pageIndex;

    KinesisReader(
            KinesisConnection.Settings connectionSettings,
            AmazonKinesisSource.ReadSettings options,
            Context context,
            boolean streaming,
            boolean checkpointing,
            String scope) {
        this.connectionSettings = connectionSettings;
        this.options = options;
        this.context = context;
        this.streaming = streaming;
        this.checkpointing = checkpointing;
        this.scope = scope;
    }

    /*
     * Only the active reader owns a client/worker. Publish them under the lifecycle lock so cancel
     * can abort without taking the engine checkpoint monitor or waiting for state collection.
     */
    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && worker == null && context.getIndexOfSubtask() == 0) {
                connection = new KinesisConnection(connectionSettings);
                worker =
                        Executors.newSingleThreadExecutor(
                                task -> {
                                    Thread thread = new Thread(task, "kinesis-source-io");
                                    thread.setDaemon(true);
                                    return thread;
                                });
            }
        }
    }

    /**
     * Poll owns output and logical progress. Network discovery and reads run without the engine
     * lock.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            synchronized (state) {
                if (closed || signaled) {
                    return;
                }
                if (split == null ? assignedNoMore : split.complete) {
                    signaled = true;
                } else {
                    if (split != null) {
                        pollAssigned(output);
                    }
                    return;
                }
            }
        }
        if (!closed) {
            context.signalNoMoreElement();
        }
    }

    /**
     * Capture logical sequence after collection; provider iterators and buffered pages stay
     * transient.
     */
    @Override
    public List<KinesisSplit> snapshotState(long checkpointId) {
        synchronized (state) {
            if (split == null) {
                return Collections.emptyList();
            }
            split.shards.values().stream()
                    .filter(shard -> shard.done && !durableParents.contains(shard.id))
                    .forEach(shard -> firstFinishedCheckpoint.putIfAbsent(shard.id, checkpointId));
            return Collections.singletonList(split.copy());
        }
    }

    /**
     * Only a successful checkpoint makes newly completed parents eligible to unblock descendants.
     * Completion notifications are serialized by the same logical state lock as snapshot and
     * polling.
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        synchronized (state) {
            firstFinishedCheckpoint
                    .entrySet()
                    .removeIf(
                            entry -> {
                                if (entry.getValue() <= checkpointId) {
                                    durableParents.add(entry.getKey());
                                    return true;
                                }
                                return false;
                            });
        }
    }

    /**
     * Install exactly one scoped split after enumerator restore/assignment, copying durable state.
     * Previously terminal parents are already durable in this restored checkpoint; buffers/tokens
     * are not restored.
     */
    @Override
    public void addSplits(List<KinesisSplit> splits) {
        if (splits.size() != 1) {
            throw invalid("invalid split assignment");
        }
        KinesisSplit restored = splits.get(0);
        restored.verify(scope);
        synchronized (state) {
            if (split != null) {
                throw invalid("duplicate active split");
            }
            split = restored.copy();
            split.shards.values().stream()
                    .filter(shard -> shard.done)
                    .forEach(shard -> durableParents.add(shard.id));
            batchDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.batchTimeout);
        }
    }

    @Override
    public void handleNoMoreSplits() {
        synchronized (state) {
            assignedNoMore = true;
        }
    }

    /**
     * Cancel without the collector checkpoint lock so held I/O and collection cannot block
     * teardown. The owned connection aborts active sockets; no new logical progress is published
     * after closure.
     */
    @Override
    public void cancel() {
        synchronized (lifecycle) {
            closed = true;
            if (worker != null) {
                worker.shutdownNow();
                connection.close();
            }
        }
    }

    @Override
    public void close() {
        cancel();
    }

    private void pollAssigned(Collector<SeaTunnelRow> output) throws Exception {
        if (!streaming && batchDeadline - System.nanoTime() <= 0) {
            cancel();
            throw invalid("batch catch-up deadline exceeded");
        }
        if (page != null) {
            emit(output);
            return;
        }
        if (request == null) {
            submitNext();
            return;
        }
        try {
            Result result = request.get(5, TimeUnit.MILLISECONDS);
            request = null;
            if (closed) {
                return;
            }
            if (result instanceof Topology) {
                install((Topology) result);
            } else {
                page = (Page) result;
                emit(output);
            }
        } catch (TimeoutException ex) {
            if (requestDeadline - System.nanoTime() <= 0) {
                cancel();
                throw invalid("read operation deadline exceeded");
            }
        } catch (ExecutionException ex) {
            if (closed) {
                return;
            }
            if (ex.getCause() instanceof KinesisConnection.ReadFailure) {
                throw (KinesisConnection.ReadFailure) ex.getCause();
            }
            if (ex.getCause() instanceof ProtocolFailure) {
                throw (ProtocolFailure) ex.getCause();
            }
            throw invalid("SDK operation could not be completed");
        }
    }

    private void submitNext() {
        long now = System.nanoTime();
        boolean refresh = !verified || (streaming && nextDiscovery - now <= 0);
        KinesisSplit.ShardState candidate = refresh ? null : nextShard(now);
        if (!refresh && candidate == null) {
            return;
        }
        synchronized (lifecycle) {
            if (closed) {
                return;
            }
            if (refresh) {
                // Token restarts and all discovery pages share this absolute elapsed budget.
                long deadline = now + TimeUnit.SECONDS.toNanos(60);
                requestDeadline = deadline;
                request = worker.submit(() -> discover(deadline));
            } else {
                Cursor cursor = cursors.computeIfAbsent(candidate.id, ignored -> new Cursor());
                lastShard = candidate.id;
                String arn = split.arn;
                Instant anchor = candidate.discoveredAtStart ? split.anchor : null;
                KinesisSplit.ShardState view = new KinesisSplit.ShardState(candidate);
                String iterator = cursor.token;
                requestDeadline =
                        now
                                + TimeUnit.MILLISECONDS.toNanos(
                                        connectionSettings.timeoutMs * 6L + 8000L);
                request = worker.submit(() -> read(arn, anchor, view, iterator));
            }
        }
    }

    /**
     * Round-robin across currently eligible shards; closed parents gate both sides of a merge.
     * Worker I/O stays outside checkpoint collection; only collected records advance durable
     * sequences.
     */
    private KinesisSplit.ShardState nextShard(long now) {
        KinesisSplit.ShardState first = null;
        boolean passedPrevious = lastShard == null;
        for (KinesisSplit.ShardState shard : split.shards.values()) {
            if (!shard.done
                    && shard.parents.stream().allMatch(this::parentReady)
                    && (cursors.get(shard.id) == null
                            || cursors.get(shard.id).eligibleAt - now <= 0)) {
                if (passedPrevious) {
                    return shard;
                }
                if (first == null) {
                    first = shard;
                }
            }
            if (shard.id.equals(lastShard)) {
                passedPrevious = true;
            }
        }
        return first;
    }

    private boolean parentReady(String id) {
        return !split.shards.containsKey(id) || durableParents.contains(id);
    }

    /*
     * Require a complete bounded listing and stable stream identity before installing logical topology.
     * Token-expiration restarts share one elapsed budget rather than restarting the whole discovery deadline.
     */
    private Topology discover(long deadline) throws InterruptedException {
        StreamDescriptionSummary summary =
                connection.read(
                        "DescribeStreamSummary",
                        () ->
                                connection
                                        .sdk
                                        .describeStreamSummary(
                                                DescribeStreamSummaryRequest.builder()
                                                        .streamName(connectionSettings.streamName)
                                                        .streamARN(connectionSettings.streamArn)
                                                        .build())
                                        .streamDescriptionSummary());
        if (summary == null
                || !matchesRequestedStream(summary.streamARN())
                || summary.streamCreationTimestamp() == null
                || (!"ACTIVE".equals(summary.streamStatusAsString())
                        && !"UPDATING".equals(summary.streamStatusAsString()))) {
            throw invalid("stream is unavailable or its identity is incomplete");
        }
        for (int attempt = 0; ; attempt++) {
            try {
                return new Topology(
                        summary.streamARN(),
                        summary.streamCreationTimestamp(),
                        discoverPages(summary.streamARN(), deadline));
            } catch (KinesisConnection.ReadFailure ex) {
                if (!ex.expiredToken() || attempt >= 2) {
                    throw ex;
                }
                KinesisConnection.waitRetry(attempt + 1);
            }
        }
    }

    private boolean matchesRequestedStream(String arn) {
        if (arn == null) {
            return false;
        }
        if (connectionSettings.streamArn != null) {
            return connectionSettings.streamArn.equals(arn);
        }
        return arn.matches(
                "arn:aws[a-z0-9-]*:kinesis:"
                        + connectionSettings.region
                        + ":[0-9]{12}:stream/"
                        + Pattern.quote(connectionSettings.streamName));
    }

    private Map<String, KinesisSplit.ShardState> discoverPages(String arn, long deadline)
            throws InterruptedException {
        Map<String, KinesisSplit.ShardState> shards = new LinkedHashMap<>();
        Set<String> seenTokens = new HashSet<>();
        String token = null;
        for (int index = 0; index < MAX_DISCOVERY_PAGES; index++) {
            if (deadline - System.nanoTime() <= 0) {
                throw invalid("shard discovery deadline exceeded");
            }
            ListShardsRequest.Builder builder = ListShardsRequest.builder().maxResults(1000);
            if (token == null) {
                builder.streamARN(arn);
            } else {
                builder.nextToken(token);
            }
            ListShardsRequest request = builder.build();
            ListShardsResponse response =
                    connection.read("ListShards", () -> connection.sdk.listShards(request));
            if (!response.hasShards()) {
                throw invalid("incomplete shard discovery response");
            }
            for (Shard item : response.shards()) {
                KinesisSplit.ShardState shard = shard(item);
                if (shards.size() >= MAX_SHARDS || shards.putIfAbsent(shard.id, shard) != null) {
                    throw invalid("duplicate shard or discovery shard budget exceeded");
                }
            }
            token = response.nextToken();
            if (token == null) {
                if (shards.isEmpty()) {
                    throw invalid("shard discovery returned no retained shards");
                }
                return shards;
            }
            if (token.isEmpty()
                    || token.length() > 1048576
                    || !seenTokens.add(fingerprint(token))) {
                throw invalid("invalid or repeated shard discovery token");
            }
        }
        throw invalid("shard discovery page budget exceeded");
    }

    private static KinesisSplit.ShardState shard(Shard item) {
        if (item == null
                || !validShard(item.shardId())
                || item.sequenceNumberRange() == null
                || !validSequence(item.sequenceNumberRange().startingSequenceNumber())
                || (item.sequenceNumberRange().endingSequenceNumber() != null
                        && !validSequence(item.sequenceNumberRange().endingSequenceNumber()))) {
            throw invalid("malformed shard metadata");
        }
        List<String> parents = new ArrayList<>(2);
        if (item.parentShardId() != null) {
            parents.add(item.parentShardId());
        }
        if (item.adjacentParentShardId() != null) {
            parents.add(item.adjacentParentShardId());
        }
        if (parents.stream().anyMatch(parent -> !validShard(parent))
                || new HashSet<>(parents).size() != parents.size()) {
            throw invalid("malformed shard parent metadata");
        }
        return new KinesisSplit.ShardState(
                item.shardId(),
                parents,
                true,
                item.sequenceNumberRange().endingSequenceNumber() != null);
    }

    /**
     * Reconcile after validating a complete listing, keeping terminal parents for lineage checks.
     */
    private void install(Topology topology) {
        boolean first = split.arn == null;
        if (!first
                && (!split.arn.equals(topology.arn) || !split.created.equals(topology.created))) {
            throw invalid("checkpoint stream identity changed");
        }
        Map<String, KinesisSplit.ShardState> merged = new LinkedHashMap<>();
        for (KinesisSplit.ShardState known : split.shards.values()) {
            KinesisSplit.ShardState current = topology.shards.get(known.id);
            if (current == null && !known.done) {
                throw invalid("unfinished shard disappeared from the retained topology");
            }
            if (current != null
                    && (!known.parents.equals(current.parents)
                            || known.closed && !current.closed)) {
                throw invalid("inconsistent shard topology");
            }
            KinesisSplit.ShardState retained = new KinesisSplit.ShardState(known);
            if (current != null) {
                retained.closed = current.closed;
            }
            merged.put(known.id, retained);
        }
        if (first || streaming) {
            for (KinesisSplit.ShardState current : topology.shards.values()) {
                if (!merged.containsKey(current.id)) {
                    merged.put(
                            current.id,
                            new KinesisSplit.ShardState(
                                    current.id, current.parents, first, current.closed));
                }
            }
        }
        if (merged.size() > MAX_SHARDS) {
            throw invalid("retained shard state budget exceeded");
        }
        checkAcyclic(merged);
        split.arn = topology.arn;
        split.created = topology.created;
        split.shards.clear();
        split.shards.putAll(merged);
        verified = true;
        nextDiscovery = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.discoveryMs);
    }

    private static void checkAcyclic(Map<String, KinesisSplit.ShardState> shards) {
        Map<String, List<String>> children = new HashMap<>();
        Map<String, Integer> indegree = new HashMap<>();
        ArrayDeque<String> ready = new ArrayDeque<>();
        for (KinesisSplit.ShardState shard : shards.values()) {
            int parents = 0;
            for (String parent : shard.parents) {
                if (shards.containsKey(parent)) {
                    children.computeIfAbsent(parent, ignored -> new ArrayList<>()).add(shard.id);
                    parents++;
                }
            }
            indegree.put(shard.id, parents);
            if (parents == 0) {
                ready.add(shard.id);
            }
        }
        int visited = 0;
        while (!ready.isEmpty()) {
            String root = ready.remove();
            visited++;
            for (String child : children.getOrDefault(root, Collections.emptyList())) {
                if (indegree.compute(child, (id, count) -> count - 1) == 0) {
                    ready.add(child);
                }
            }
        }
        if (visited != shards.size()) {
            throw invalid("shard lineage contains a cycle");
        }
    }

    /*
     * Reacquire transient iterators after the durable sequence or preserved timestamp anchor.
     * Validate the entire page before the engine can emit any of its records or advance a checkpoint.
     */
    private Page read(String arn, Instant anchor, KinesisSplit.ShardState shard, String token)
            throws InterruptedException {
        String iterator = token;
        if (iterator == null) {
            GetShardIteratorRequest.Builder builder =
                    GetShardIteratorRequest.builder().streamARN(arn).shardId(shard.id);
            if (shard.committed != null) {
                builder.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                        .startingSequenceNumber(shard.committed);
            } else if (anchor != null) {
                builder.shardIteratorType(ShardIteratorType.AT_TIMESTAMP).timestamp(anchor);
            } else {
                builder.shardIteratorType(ShardIteratorType.TRIM_HORIZON);
            }
            GetShardIteratorRequest request = builder.build();
            iterator =
                    connection.read(
                            "GetShardIterator",
                            () -> connection.sdk.getShardIterator(request).shardIterator());
            requireIterator(iterator);
        }
        GetRecordsRequest request =
                GetRecordsRequest.builder().shardIterator(iterator).limit(options.records).build();
        GetRecordsResponse response;
        try {
            response = connection.read("GetRecords", () -> connection.sdk.getRecords(request));
        } catch (KinesisConnection.ReadFailure ex) {
            if (ex.expiredIterator()) {
                return Page.expired(shard.id);
            }
            throw ex;
        }
        if (!response.hasRecords()
                || response.records().size() > options.records
                || (response.millisBehindLatest() != null && response.millisBehindLatest() < 0)) {
            throw invalid("malformed records response");
        }
        if (response.nextShardIterator() != null) {
            requireIterator(response.nextShardIterator());
            if (!response.childShards().isEmpty()) {
                throw invalid("unexpected child hint before shard exhaustion");
            }
        }
        for (ChildShard child : response.childShards()) {
            if (!validShard(child.shardId())
                    || !child.parentShards().contains(shard.id)
                    || child.parentShards().size() > 2
                    || child.parentShards().stream().anyMatch(parent -> !validShard(parent))) {
                throw invalid("malformed terminal child hint");
            }
        }
        String last = shard.committed;
        long bytes = 0;
        for (Record record : response.records()) {
            if (record == null
                    || record.data() == null
                    || !validSequence(record.sequenceNumber())
                    || (last != null && compareSequence(last, record.sequenceNumber()) >= 0)) {
                throw invalid("missing, repeated or unordered record sequence or payload");
            }
            last = record.sequenceNumber();
            bytes += record.data().asByteBuffer().remaining();
            if (bytes > 10485760) {
                throw invalid("records response exceeds the data budget");
            }
        }
        return new Page(
                shard.id,
                response.records(),
                response.nextShardIterator(),
                Long.valueOf(0).equals(response.millisBehindLatest()),
                bytes,
                false);
    }

    private static void requireIterator(String iterator) {
        if (iterator == null || iterator.isEmpty() || iterator.length() > 512) {
            throw invalid("invalid shard iterator");
        }
    }

    /*
     * Advance a durable sequence only after collection returns; a partially emitted page can replay
     * from that sequence after recovery. Parent completion becomes durable through checkpoint notification.
     */
    private void emit(Collector<SeaTunnelRow> output) {
        KinesisSplit.ShardState shard = split.shards.get(page.shard);
        Cursor cursor = cursors.get(page.shard);
        if (page.expired) {
            cursor.token = null;
            cursor.eligibleAt =
                    System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.shardPollMs);
            if (++cursor.expirations > 3) {
                throw invalid("shard iterator repeatedly expired");
            }
            page = null;
            return;
        }
        int end = Math.min(page.records.size(), pageIndex + ROWS_PER_POLL);
        while (!closed && pageIndex < end) {
            Record record = page.records.get(pageIndex);
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                split.arn,
                                shard.id,
                                record.sequenceNumber(),
                                record.partitionKey(),
                                record.approximateArrivalTimestamp() == null
                                        ? null
                                        : record.approximateArrivalTimestamp().toString(),
                                record.data().asByteArray()
                            });
            row.setTableId(TablePath.DEFAULT.getFullName());
            output.collect(row);
            shard.committed = record.sequenceNumber();
            pageIndex++;
        }
        if (closed || pageIndex < page.records.size()) {
            return;
        }
        if (page.next == null
                || (!streaming && !shard.closed && page.records.isEmpty() && page.caughtUp)) {
            shard.done = true;
            cursors.remove(shard.id);
            if (!checkpointing) {
                durableParents.add(shard.id);
            }
            if (streaming) {
                nextDiscovery = 0;
            } else if (split.shards.values().stream().allMatch(value -> value.done)) {
                split.complete = true;
            }
        } else {
            cursor.token = page.next;
            cursor.expirations = 0;
            long pause =
                    page.bytes >= 10000000
                            ? Math.max(options.shardPollMs, 5000)
                            : options.shardPollMs;
            cursor.eligibleAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(pause);
        }
        page = null;
        pageIndex = 0;
    }

    private static boolean validShard(String value) {
        return value != null && value.matches("[A-Za-z0-9_.-]{1,128}");
    }

    private static boolean validSequence(String value) {
        return value != null && value.matches("0|[1-9][0-9]{0,128}");
    }

    private static int compareSequence(String first, String second) {
        return first.length() == second.length()
                ? first.compareTo(second)
                : Integer.compare(first.length(), second.length());
    }

    private static String fingerprint(String value) {
        try {
            return Base64.getEncoder()
                    .encodeToString(
                            MessageDigest.getInstance("SHA-256")
                                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException ex) {
            throw invalid("cannot digest discovery token");
        }
    }

    private static ProtocolFailure invalid(String text) {
        return new ProtocolFailure("Kinesis source: " + text);
    }

    /**
     * Carries local protocol validation text across the worker boundary without provider response
     * content. Unexpected SDK failures are sanitized before they reach the engine task.
     */
    private static final class ProtocolFailure extends IllegalStateException {
        private ProtocolFailure(String message) {
            super(message);
        }
    }

    /**
     * Marks an immutable worker result accepted by the engine thread under the state/checkpoint
     * locks. Workers never mutate the durable split or collect rows directly.
     */
    private interface Result {}

    /**
     * Represents a complete validated listing before it is reconciled into the durable shard graph.
     * A partial page sequence never replaces checkpointed topology.
     */
    private static final class Topology implements Result {
        private final String arn;
        private final Instant created;
        private final Map<String, KinesisSplit.ShardState> shards;

        private Topology(String arn, Instant created, Map<String, KinesisSplit.ShardState> shards) {
            this.arn = arn;
            this.created = created;
            this.shards = shards;
        }
    }

    /**
     * Holds transient provider iterator, poll timing and expiration counters for one shard. Restore
     * discards these fields and reacquires an iterator from the durable sequence or anchor.
     */
    private static final class Cursor {
        private String token;
        private long eligibleAt;
        private int expirations;
    }

    /**
     * Transfers one bounded, validated provider page from the worker to the engine polling thread.
     * Only records actually collected advance the durable sequence; unread page data stays
     * transient.
     */
    private static final class Page implements Result {
        private final String shard;
        private final List<Record> records;
        private final String next;
        private final boolean caughtUp;
        private final long bytes;
        private final boolean expired;

        private Page(
                String shard,
                List<Record> records,
                String next,
                boolean caughtUp,
                long bytes,
                boolean expired) {
            this.shard = shard;
            this.records = records;
            this.next = next;
            this.caughtUp = caughtUp;
            this.bytes = bytes;
            this.expired = expired;
        }

        private static Page expired(String shard) {
            return new Page(shard, Collections.emptyList(), null, false, 0, true);
        }
    }
}
