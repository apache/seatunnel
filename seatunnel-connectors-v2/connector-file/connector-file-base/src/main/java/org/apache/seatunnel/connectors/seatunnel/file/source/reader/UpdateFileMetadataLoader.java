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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.connectors.seatunnel.file.hadoop.FileStatusListingSession;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import lombok.Getter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

final class UpdateFileMetadataLoader {
    private static final int IN_FLIGHT_MULTIPLIER = 8;

    private UpdateFileMetadataLoader() {}

    /**
     * Loads target metadata using one directory list for dense groups and bounded point lookups.
     */
    static Result load(
            List<Request> requests,
            HadoopFileSystemProxy target,
            int parallelism,
            int bulkThreshold,
            boolean alwaysBulk)
            throws IOException {
        FileStatus[] ordered = new FileStatus[requests.size()];
        Map<Path, List<Request>> groups = new LinkedHashMap<>();
        List<Request> pointRequests = new ArrayList<>();
        for (Request request : requests) {
            Path parent = new Path(request.targetPath).getParent();
            if (parent == null || parent.toString().isEmpty()) {
                pointRequests.add(request);
            } else {
                groups.computeIfAbsent(parent, ignored -> new ArrayList<>()).add(request);
            }
        }

        List<Map.Entry<Path, List<Request>>> bulkGroups = new ArrayList<>();
        for (Map.Entry<Path, List<Request>> group : groups.entrySet()) {
            if (!alwaysBulk && group.getValue().size() < bulkThreshold) {
                pointRequests.addAll(group.getValue());
            } else {
                bulkGroups.add(group);
            }
        }

        long bulkNanos = 0;
        if (!bulkGroups.isEmpty()) {
            try (FileStatusListingSession session = target.openFileStatusListingSession()) {
                for (Map.Entry<Path, List<Request>> group : bulkGroups) {
                    long started = System.nanoTime();
                    Map<String, Request> wanted = new HashMap<>();
                    for (Request request : group.getValue()) {
                        wanted.put(new Path(request.targetPath).getName(), request);
                    }
                    try {
                        session.list(
                                group.getKey(),
                                status -> {
                                    Request request = wanted.get(status.getPath().getName());
                                    if (request != null) {
                                        ordered[request.order] = status;
                                    }
                                });
                    } catch (FileNotFoundException ignored) {
                        // A missing target directory means every source file in the group is new.
                    } catch (IOException e) {
                        throw new IOException(
                                "Failed during target_bulk_listing for path="
                                        + mask(group.getKey()),
                                e);
                    } finally {
                        bulkNanos += System.nanoTime() - started;
                    }
                }
            }
        }

        PointResult pointResult = loadPoints(pointRequests, target, ordered, parallelism);
        return new Result(
                Arrays.asList(ordered),
                bulkGroups.size(),
                pointRequests.size(),
                pointResult.peakConcurrency,
                pointResult.peakInFlight,
                bulkNanos,
                pointResult.elapsedNanos);
    }

    private static PointResult loadPoints(
            List<Request> requests,
            HadoopFileSystemProxy target,
            FileStatus[] ordered,
            int parallelism)
            throws IOException {
        if (requests.isEmpty()) {
            return new PointResult(0, 0, 0);
        }
        ExecutorService executor = Executors.newFixedThreadPool(parallelism);
        CompletionService<Lookup> completion = new ExecutorCompletionService<>(executor);
        Set<Future<Lookup>> inFlight = new HashSet<>();
        int maxInFlight = parallelism * IN_FLIGHT_MULTIPLIER;
        int submitted = 0;
        int completed = 0;
        int peakInFlight = 0;
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peakConcurrency = new AtomicInteger();
        long started = System.nanoTime();
        IOException failure = null;
        try {
            while (completed < requests.size()) {
                while (submitted < requests.size() && submitted - completed < maxInFlight) {
                    Request request = requests.get(submitted++);
                    inFlight.add(
                            completion.submit(
                                    () -> {
                                        int current = active.incrementAndGet();
                                        peakConcurrency.accumulateAndGet(current, Math::max);
                                        try {
                                            return new Lookup(
                                                    request,
                                                    target.getFileStatus(request.targetPath));
                                        } catch (FileNotFoundException ignored) {
                                            return new Lookup(request, null);
                                        } catch (IOException e) {
                                            throw new IOException(
                                                    "Target metadata lookup failed for path="
                                                            + mask(new Path(request.targetPath)),
                                                    e);
                                        } finally {
                                            active.decrementAndGet();
                                        }
                                    }));
                    peakInFlight = Math.max(peakInFlight, submitted - completed);
                }
                try {
                    Future<Lookup> completedFuture = completion.take();
                    inFlight.remove(completedFuture);
                    Lookup lookup = completedFuture.get();
                    ordered[lookup.request.order] = lookup.status;
                    completed++;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    failure = new IOException("Interrupted during target_point_lookup", e);
                    throw failure;
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    failure = new IOException("Failed during target_point_lookup", cause);
                    throw failure;
                }
            }
            return new PointResult(
                    peakConcurrency.get(), peakInFlight, System.nanoTime() - started);
        } finally {
            for (Future<Lookup> future : inFlight) {
                future.cancel(true);
            }
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    IOException cleanupFailure =
                            new IOException("Timed out closing target_point_lookup workers");
                    if (failure != null) {
                        failure.addSuppressed(cleanupFailure);
                    } else {
                        throw cleanupFailure;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (failure != null) {
                    failure.addSuppressed(e);
                } else {
                    throw new IOException("Interrupted closing target_point_lookup workers", e);
                }
            }
        }
    }

    @Getter
    static final class Request {
        private final int order;
        private final String targetPath;

        Request(int order, String targetPath) {
            this.order = order;
            this.targetPath = targetPath;
        }
    }

    @Getter
    static final class Result {
        private final List<FileStatus> statuses;
        private final int bulkListedDirectories;
        private final int pointLookups;
        private final int peakConcurrency;
        private final int peakInFlight;
        private final long bulkListingNanos;
        private final long pointLookupNanos;

        Result(
                List<FileStatus> statuses,
                int bulkListedDirectories,
                int pointLookups,
                int peakConcurrency,
                int peakInFlight,
                long bulkListingNanos,
                long pointLookupNanos) {
            this.statuses = statuses;
            this.bulkListedDirectories = bulkListedDirectories;
            this.pointLookups = pointLookups;
            this.peakConcurrency = peakConcurrency;
            this.peakInFlight = peakInFlight;
            this.bulkListingNanos = bulkListingNanos;
            this.pointLookupNanos = pointLookupNanos;
        }
    }

    private static final class Lookup {
        private final Request request;
        private final FileStatus status;

        private Lookup(Request request, FileStatus status) {
            this.request = request;
            this.status = status;
        }
    }

    private static final class PointResult {
        private final int peakConcurrency;
        private final int peakInFlight;
        private final long elapsedNanos;

        private PointResult(int peakConcurrency, int peakInFlight, long elapsedNanos) {
            this.peakConcurrency = peakConcurrency;
            this.peakInFlight = peakInFlight;
            this.elapsedNanos = elapsedNanos;
        }
    }

    private static String mask(Path path) {
        java.net.URI uri = path.toUri();
        if (uri.getUserInfo() == null || uri.getAuthority() == null) {
            return path.toString();
        }
        return path.toString().replace(uri.getUserInfo() + "@", "***@");
    }
}
