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
package org.apache.seatunnel.connectors.seatunnel.auth0;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Reads one bounded tenant scan, including both levels of organization children.
 *
 * <p>Network I/O runs outside checkpoint locks. Each emission and its cursor advance occur under
 * the same collector lock as the engine's checkpoint.
 */
final class Auth0Reader implements SourceReader<SeaTunnelRow, Auth0Split> {
    private final Auth0Config config;
    private final Context context;
    private final Object checkpoint = new Object();
    private final Object lifecycle = new Object();

    private volatile boolean closed;
    private boolean idle;
    private boolean signaled;

    // Only the poll thread mutates progress or caches pages once a split is assigned.
    private Auth0Split progress;
    private Auth0Api.Page topPage;
    private Auth0Api.Page memberPage;
    private Auth0Api.Page rolePage;

    // Cancellation closes the socket and interrupts the only in-flight operation.
    private Auth0Http http;
    private Auth0Api api;
    private ExecutorService executor;
    private Future<?> pending;
    private long expires;

    Auth0Reader(Auth0Config config, Context context) {
        this.config = config;
        this.context = context;
    }

    /**
     * Allocates the transport for the worker that owns the tenant split.
     *
     * <p>A cancellation before open prevents future network work.
     */
    @Override
    public void open() {
        synchronized (lifecycle) {
            if (!closed && executor == null && context.getIndexOfSubtask() == 0) {
                http = new Auth0Http(config);
                api = new Auth0Api(config, http);
                executor =
                        Executors.newSingleThreadExecutor(
                                task -> {
                                    Thread thread = new Thread(task, "auth0-source");
                                    thread.setDaemon(true);
                                    return thread;
                                });
            }
        }
    }

    /**
     * Completes one bounded transition, permitting savepoints during HTTPS reads.
     *
     * <p>The chosen organization and member are committed before child requests begin.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (checkpoint) {
            if (closed || signaled) {
                return;
            }
            if (progress == null ? idle : progress.finished()) {
                signaled = true;
                context.signalNoMoreElement();
                return;
            }
            if (progress == null) {
                return;
            }
        }
        switch (progress.phase) {
            case CLIENTS:
                clients(output);
                break;
            case USERS:
                users(output);
                break;
            case ORGANIZATIONS:
                organizations(output);
                break;
            default:
                throw new IllegalStateException("Invalid Auth0 scan phase");
        }
    }

    private void clients(Collector<SeaTunnelRow> output) throws Exception {
        Auth0Resource resource = Auth0Resource.CLIENTS;
        Auth0Split.Position position = progress.top;
        if (topPage == null) {
            topPage = readPage(output, resource, position, null, null, null);
        } else if (position.row < topPage.rows.size()) {
            commit(output, () -> emitNext(output, resource, position, topPage, null, null));
        } else {
            commit(
                    output,
                    () -> {
                        if (topPage.full) {
                            position.advance(resource, topPage);
                        } else {
                            progress.completePhase(config);
                        }
                        topPage = null;
                    });
        }
    }

    private void users(Collector<SeaTunnelRow> output) throws Exception {
        Auth0Resource resource = Auth0Resource.USERS;
        Auth0Split.Window window = progress.windows.getFirst();
        if (window.count < 0) {
            progress.requireBudget(resource, config.maxPages);
            Integer size = get(resource, Integer.class, deadline -> api.count(window, deadline));
            if (size != null) {
                commit(
                        output,
                        () -> {
                            progress.recordRequest(resource);
                            progress.setWindowCount(size);
                        });
            }
            return;
        }
        if (window.count == 0) {
            commit(output, () -> progress.finishWindow(config));
            return;
        }
        Auth0Split.Position position = progress.top;
        if (topPage == null) {
            topPage = readPage(output, resource, position, window, null, null);
        } else if (position.row < topPage.rows.size()) {
            commit(output, () -> emitNext(output, resource, position, topPage, null, null));
        } else {
            commit(
                    output,
                    () -> {
                        if ((long) position.number * config.pageSize + position.row
                                < window.count) {
                            position.advance(resource, topPage);
                        } else {
                            progress.finishWindow(config);
                        }
                        topPage = null;
                    });
        }
    }

    // A parent row can be selected independently of the paths that require its identifier.
    private void organizations(Collector<SeaTunnelRow> output) throws Exception {
        Auth0Resource resource = Auth0Resource.ORGANIZATIONS;
        Auth0Split.Position position = progress.top;
        if (topPage == null) {
            topPage = readPage(output, resource, position, null, null, null);
        } else if (position.row == topPage.rows.size()) {
            commit(
                    output,
                    () -> {
                        if (topPage.next == null) {
                            progress.completePhase(config);
                        } else {
                            position.advance(resource, topPage);
                        }
                        topPage = null;
                    });
        } else if (progress.organizationId == null) {
            Auth0Api.Row organization = topPage.rows.get(position.row);
            commit(
                    output,
                    () -> {
                        if (config.includes(resource)) {
                            emit(output, resource, organization, null, null);
                        }
                        if (config.needsMembers()) {
                            progress.organizationId = organization.id;
                        } else {
                            ++position.row;
                        }
                    });
        } else {
            members(output);
        }
    }

    private void members(Collector<SeaTunnelRow> output) throws Exception {
        Auth0Resource resource = Auth0Resource.ORGANIZATION_MEMBERS;
        Auth0Split.Position position = progress.members;
        String organization = progress.organizationId;
        if (memberPage == null) {
            memberPage = readPage(output, resource, position, null, organization, null);
        } else if (position.row == memberPage.rows.size()) {
            commit(
                    output,
                    () -> {
                        if (memberPage.next != null) {
                            position.advance(resource, memberPage);
                        } else {
                            progress.organizationId = null;
                            progress.members = new Auth0Split.Position();
                            ++progress.top.row;
                        }
                        memberPage = null;
                    });
        } else if (progress.userId == null) {
            Auth0Api.Row member = memberPage.rows.get(position.row);
            commit(
                    output,
                    () -> {
                        if (config.includes(resource)) {
                            emit(output, resource, member, organization, member.id);
                        }
                        if (config.includes(Auth0Resource.ORGANIZATION_MEMBER_ROLES)) {
                            progress.userId = member.id;
                        } else {
                            ++position.row;
                        }
                    });
        } else {
            roles(output, organization, progress.userId);
        }
    }

    private void roles(Collector<SeaTunnelRow> output, String organization, String user)
            throws Exception {
        Auth0Resource resource = Auth0Resource.ORGANIZATION_MEMBER_ROLES;
        Auth0Split.Position position = progress.roles;
        if (rolePage == null) {
            rolePage = readPage(output, resource, position, null, organization, user);
        } else if (position.row < rolePage.rows.size()) {
            commit(
                    output,
                    () -> emitNext(output, resource, position, rolePage, organization, user));
        } else {
            commit(
                    output,
                    () -> {
                        if (rolePage.full) {
                            // The direct member roles endpoint only accepts offset page indices 0
                            // to 1000.
                            if (position.number == 1000) {
                                throw resource.failure(
                                        "member role pagination reached the provider page limit");
                            }
                            position.advance(resource, rolePage);
                        } else {
                            progress.roles = new Auth0Split.Position();
                            progress.userId = null;
                            ++progress.members.row;
                        }
                        rolePage = null;
                    });
        }
    }

    /**
     * Reconstructs and checks a saved page before allowing any more records from it.
     *
     * <p>Page budget is charged once after a logical request succeeds. Restoring a previously
     * charged page compares its fingerprint and current ancestor identity.
     */
    private Auth0Api.Page readPage(
            Collector<SeaTunnelRow> output,
            Auth0Resource resource,
            Auth0Split.Position position,
            Auth0Split.Window window,
            String organization,
            String user)
            throws Exception {
        if (position.digest == null) {
            progress.requireBudget(resource, config.maxPages);
        }
        Auth0Api.Page page =
                get(
                        resource,
                        Auth0Api.Page.class,
                        deadline ->
                                api.page(resource, position, window, organization, user, deadline));
        if (page == null) {
            return null;
        }
        if (position.row > page.rows.size()
                || position.digest != null && !position.digest.equals(page.digest)) {
            throw resource.failure("page changed since checkpoint");
        }
        String activeChild =
                resource == Auth0Resource.ORGANIZATIONS
                        ? progress.organizationId
                        : resource == Auth0Resource.ORGANIZATION_MEMBERS ? progress.userId : null;
        if (activeChild != null
                && (position.row == page.rows.size()
                        || !activeChild.equals(page.rows.get(position.row).id))) {
            throw resource.failure("parent identity changed since checkpoint");
        }
        if (page.rows.stream()
                .anyMatch(row -> position.previous.contains(Auth0Config.hash(row.id)))) {
            throw resource.failure("page repeats a record identity from earlier pagination");
        }
        if (window != null) {
            long remaining = window.count - (long) position.number * config.pageSize;
            if (page.rows.size() != Math.min(config.pageSize, remaining)) {
                throw resource.failure("user page count differs from search totals");
            }
            if (!page.rows.isEmpty() && page.rows.get(0).updated < position.previousUpdate) {
                throw resource.failure("users pages are not ordered by updated_at");
            }
        }
        if (position.digest == null) {
            commit(
                    output,
                    () -> {
                        position.digest = page.digest;
                        progress.recordRequest(resource);
                    });
        }
        return page;
    }

    private static void emitNext(
            Collector<SeaTunnelRow> output,
            Auth0Resource resource,
            Auth0Split.Position position,
            Auth0Api.Page page,
            String organization,
            String user) {
        emit(output, resource, page.rows.get(position.row), organization, user);
        ++position.row;
    }

    private static void emit(
            Collector<SeaTunnelRow> output,
            Auth0Resource resource,
            Auth0Api.Row value,
            String organization,
            String user) {
        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            resource.label, value.id, organization, user, value.record.toString()
                        });
        row.setTableId(TablePath.DEFAULT.getFullName());
        output.collect(row);
    }

    private <T> T get(Auth0Resource resource, Class<T> kind, Network<T> work) throws Exception {
        Future<?> request;
        synchronized (lifecycle) {
            if (closed) {
                return null;
            }
            if (pending == null) {
                expires = http.deadline();
                long deadline = expires;
                Callable<T> task = () -> work.read(deadline);
                pending = executor.submit(task);
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
            if (!closed && expires - System.nanoTime() <= 0) {
                cancel();
                throw resource.failure("HTTPS operation deadline exceeded");
            }
            return null;
        } catch (CancellationException ex) {
            if (closed) {
                return null;
            }
            throw resource.failure("read canceled");
        } catch (ExecutionException ex) {
            if (closed) {
                return null;
            }
            if (ex.getCause() instanceof Auth0Resource.Failure) {
                throw (Auth0Resource.Failure) ex.getCause();
            }
            throw resource.failure("provider response could not be read");
        }
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
     * Captures a detached position after the current accepted row.
     *
     * <p>Network bytes in progress and access tokens have no checkpoint representation.
     */
    @Override
    public List<Auth0Split> snapshotState(long checkpointId) {
        synchronized (checkpoint) {
            return progress == null ? List.of() : List.of(progress.copy());
        }
    }

    /**
     * Accepts one account scan on the active subtask and verifies its logical scope.
     *
     * <p>Restore preserves the original frozen user search bounds.
     */
    @Override
    public void addSplits(List<Auth0Split> splits) {
        synchronized (checkpoint) {
            if (splits.isEmpty()) {
                return;
            }
            if (splits.size() != 1 || progress != null || context.getIndexOfSubtask() != 0) {
                throw new IllegalStateException("Invalid Auth0 reader assignment");
            }
            Auth0Split split = splits.get(0);
            split.validate(config);
            progress = split.copy();
        }
    }

    @Override
    public void handleNoMoreSplits() {
        synchronized (checkpoint) {
            idle = true;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    /**
     * Aborts current HTTPS and prevents later work on the canceled reader.
     *
     * <p>The source checkpoint lock is independent from this hook.
     */
    @Override
    public void cancel() {
        synchronized (lifecycle) {
            closed = true;
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
    }

    @Override
    public void close() {
        cancel();
    }

    /**
     * Carries one bounded remote read and leaves state updates on the engine poll thread.
     *
     * <p>One absolute deadline applies to authentication and all attempts of the operation.
     */
    private interface Network<T> {
        T read(long deadline) throws InterruptedException;
    }
}
