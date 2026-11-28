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

package org.apache.seatunnel.connectors.seatunnel.bamboohr;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRConfig.Bounds;
import org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRConfig.Resource;
import org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRJson.EmployeeRef;
import org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRJson.Failure;
import org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSource.Cursor;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Executes the selected resource traversal, with only one provider response or employee detail
 * request outstanding. Checkpoints capture emitted progress instead of retrieval futures.
 */
final class BambooHRReader implements SourceReader<SeaTunnelRow, Cursor> {
    private static final int ROWS_PER_POLL = 16;
    private final BambooHRConfig config;
    private final Context context;
    private final BambooHRHttp http;
    private final String table;
    private final Object progress = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "seatunnel-bamboohr-retrieval");
                        thread.setDaemon(true);
                        return thread;
                    });

    private Bounds bounds;
    private LocalDate windowStart;
    private int offset;
    private String digest;
    private boolean finished;
    private boolean eof;
    private List<EmployeeRef> employees;
    private Batch batch;
    private volatile Future<Batch> inFlight;

    BambooHRReader(BambooHRConfig config, Context context, String table) {
        this.config = config;
        this.context = context;
        this.table = table;
        this.http = new BambooHRHttp(config);
    }

    @Override
    public void open() {}

    /**
     * Polls completed retrieval work without blocking the engine's checkpoint lock on network I/O.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws IOException {
        synchronized (output.getCheckpointLock()) {
            synchronized (progress) {
                if (closed.get() || bounds == null || eof) {
                    return;
                }
                if (finished) {
                    eof = true;
                    context.signalNoMoreElement();
                    return;
                }
                try {
                    if (batch != null || prepareBatch()) {
                        emit(output);
                    }
                } catch (Failure e) {
                    throw new IOException(
                            "BambooHR resource " + config.resource.id + ": " + e.getMessage());
                }
            }
        }
    }

    /**
     * Starts one immutable retrieval plan, or publishes its result after verifying restored state.
     */
    private boolean prepareBatch() throws IOException {
        Future<Batch> task = inFlight;
        if (task == null) {
            submit();
            return false;
        }
        if (!task.isDone()) {
            return false;
        }
        try {
            Batch received = task.get();
            inFlight = null;
            if (closed.get()) {
                return false;
            }
            if (received.employees != null) {
                verifyReplay(
                        offset,
                        digest,
                        received.digest,
                        received.employees.size(),
                        "employee discovery list");
                employees = received.employees;
                digest = offset > 0 ? received.digest : null;
                if (employees.isEmpty()) {
                    finish();
                }
                return false;
            }
            if (config.resource != Resource.EMPLOYEES) {
                verifyReplay(
                        offset, digest, received.digest, received.rows.size(), "response rows");
            }
            batch = received;
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (!closed.get()) {
                throw BambooHRJson.failure("retrieval interrupted");
            }
        } catch (CancellationException e) {
            if (!closed.get()) {
                throw BambooHRJson.failure("retrieval canceled");
            }
        } catch (ExecutionException e) {
            if (!closed.get()) {
                if (e.getCause() instanceof Failure) {
                    throw (Failure) e.getCause();
                }
                throw BambooHRJson.failure("retrieval worker failed");
            }
        }
        return false;
    }

    static void verifyReplay(int offset, String digest, String received, int size, String entity)
            throws IOException {
        if (offset > 0 && (offset >= size || !digest.equals(received))) {
            throw BambooHRJson.failure("the " + entity + " changed since its checkpoint");
        }
    }

    private void submit() throws IOException {
        Bounds scan = bounds;
        LocalDate first = windowStart;
        EmployeeRef employee = employees == null ? null : employees.get(offset);
        try {
            inFlight = worker.submit(() -> fetch(scan, first, employee));
        } catch (RejectedExecutionException e) {
            if (!closed.get()) {
                throw BambooHRJson.failure("retrieval worker is unavailable");
            }
        }
    }

    private Batch fetch(Bounds scan, LocalDate first, EmployeeRef employee) throws IOException {
        switch (config.resource) {
            case REPORTS:
                return Batch.rows(
                        BambooHRJson.records(
                                http.fetch("reports/custom", BambooHRJson.report(config.fields)),
                                "employees",
                                config.maxRecords));
            case DIRECTORY:
                return Batch.rows(
                        BambooHRJson.records(
                                http.fetch("employees/directory", null),
                                "employees",
                                config.maxRecords));
            case META:
                return Batch.rows(
                        BambooHRJson.records(
                                http.fetch("meta/fields", null), null, config.maxRecords));
            case TIME_OFF:
                return dated(first, scan.until, false);
            case TIMESHEETS:
                if (first.isBefore(LocalDate.now(config.zone).minusDays(364))) {
                    throw BambooHRJson.failure(
                            "the saved timesheet window is outside current retention");
                }
                return dated(first, monthEnd(first, scan.until), true);
            case EMPLOYEES:
                return employee == null ? discover(scan) : detail(employee);
            default:
                throw BambooHRJson.failure("unsupported resource");
        }
    }

    private Batch dated(LocalDate first, LocalDate last, boolean timesheets) throws IOException {
        String path = timesheets ? "time_tracking/timesheet_entries" : "time_off/requests";
        List<JsonNode> source =
                BambooHRJson.records(
                        http.fetch(path + "?start=" + first + "&end=" + last, null),
                        null,
                        config.maxRecords);
        List<JsonNode> rows = new ArrayList<>();
        for (JsonNode row : source) {
            if (timesheets
                    ? BambooHRJson.inTimesheetWindow(row, first, last, config.zone)
                    : BambooHRJson.inTimeOffWindow(row, first, last)) {
                rows.add(row);
            }
        }
        return Batch.rows(rows);
    }

    private Batch discover(Bounds scan) throws IOException {
        List<EmployeeRef> refs;
        if (config.employeeChangedSince == null) {
            refs =
                    BambooHRJson.directory(
                            http.fetch("employees/directory", null), config.maxRecords);
        } else {
            JsonNode changed =
                    http.fetch(
                            "employees/changed?since="
                                    + query(config.employeeChangedSince.minusSeconds(1).toString()),
                            null);
            refs =
                    BambooHRJson.changed(
                            changed,
                            config.maxRecords,
                            config.employeeChangedSince,
                            scan.initialized);
        }
        return Batch.discovery(refs);
    }

    private Batch detail(EmployeeRef employee) throws IOException {
        if (employee.deleted()) {
            return Batch.detail(BambooHRJson.tombstone(employee.id), employee);
        }
        JsonNode response =
                http.fetch(
                        "employees/"
                                + employee.id
                                + "?fields="
                                + query(String.join(",", config.fields)),
                        null);
        return Batch.detail(
                BambooHRJson.asJson(BambooHRJson.employee(response, employee.id)), employee);
    }

    private static String query(String input) {
        return URLEncoder.encode(input, StandardCharsets.UTF_8);
    }

    static LocalDate monthEnd(LocalDate first, LocalDate last) {
        LocalDate nextBoundary = first.withDayOfMonth(first.lengthOfMonth());
        return nextBoundary.isAfter(last) ? last : nextBoundary;
    }

    private void emit(Collector<SeaTunnelRow> output) {
        if (config.resource == Resource.EMPLOYEES) {
            if (closed.get()) {
                return;
            }
            emitRow(output, batch.rows.get(0), batch.employee.action, batch.employee.changedAt);
            offset++;
            if (offset == employees.size()) {
                finish();
            } else if (offset == 1) {
                digest = BambooHRJson.digestEmployees(employees);
            }
            batch = null;
            return;
        }
        int stop = Math.min(batch.rows.size(), offset + ROWS_PER_POLL);
        while (offset < stop && !closed.get()) {
            emitRow(output, batch.rows.get(offset), null, null);
            offset++;
            digest = batch.digest;
        }
        if (offset == batch.rows.size()) {
            LocalDate next =
                    config.resource == Resource.TIMESHEETS
                            ? monthEnd(windowStart, bounds.until).plusDays(1)
                            : null;
            if (next != null && !next.isAfter(bounds.until)) {
                windowStart = next;
                offset = 0;
                digest = null;
            } else {
                finish();
            }
            batch = null;
        }
    }

    private void emitRow(Collector<SeaTunnelRow> output, String json, String action, String time) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {json, action, time});
        row.setTableId(table);
        output.collect(row);
    }

    private void finish() {
        windowStart = null;
        offset = 0;
        digest = null;
        finished = true;
    }

    /**
     * Empty status before assignment belongs to the enumerator. Terminal progress is retained to
     * prevent a completed resource from reading the provider again after restore.
     */
    @Override
    public List<Cursor> snapshotState(long checkpointId) {
        synchronized (progress) {
            if (bounds == null) {
                return List.of();
            }
            return List.of(new Cursor(config.scope, bounds, windowStart, offset, digest, finished));
        }
    }

    /**
     * Restores only the cursor. A partially consumed response is refetched and verified separately.
     */
    @Override
    public void addSplits(List<Cursor> splits) {
        synchronized (progress) {
            if (bounds != null || splits.size() != 1) {
                throw BambooHRConfig.invalid("expected one assigned BambooHR split");
            }
            Cursor cursor = splits.get(0);
            validate(config, cursor);
            bounds = cursor.bounds;
            windowStart = cursor.windowStart;
            offset = cursor.offset;
            digest = cursor.digest;
            finished = cursor.finished;
        }
    }

    static void validate(BambooHRConfig config, Cursor cursor) {
        if (cursor == null || !config.scope.equals(cursor.scope)) {
            throw BambooHRConfig.invalid(
                    "resource, company, fields or dates changed during restore");
        }
        if (cursor.bounds == null
                || cursor.bounds.initialized == null
                || (config.resource.isDated()
                        && (cursor.bounds.from == null
                                || cursor.bounds.until == null
                                || cursor.bounds.from.isAfter(cursor.bounds.until)))
                || cursor.offset < 0
                || cursor.offset >= config.maxRecords
                || ((cursor.offset == 0) != (cursor.digest == null))
                || (cursor.finished && (cursor.windowStart != null || cursor.offset != 0))
                || (!cursor.finished
                        && config.resource.isDated()
                        && (cursor.windowStart == null
                                || cursor.windowStart.isBefore(cursor.bounds.from)
                                || cursor.windowStart.isAfter(cursor.bounds.until)))
                || (!config.resource.isDated() && cursor.windowStart != null)) {
            throw BambooHRConfig.invalid("invalid BambooHR checkpoint position");
        }
    }

    @Override
    public void handleNoMoreSplits() {}

    @Override
    public void cancel() {
        close();
    }

    /**
     * Cancels the provider request or retry wait without acquiring the checkpoint lock.
     *
     * <p>Cancellation stops external I/O even when the engine is taking a snapshot.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            http.close();
            Future<Batch> pending = inFlight;
            if (pending != null) {
                pending.cancel(true);
            }
            worker.shutdownNow();
        }
    }

    /**
     * A validated set of rows, one employee detail, or the transient employee discovery map.
     *
     * <p>An employee request retains only the current detail and the bounded discovery list.
     */
    private static final class Batch {
        final List<String> rows;
        final List<EmployeeRef> employees;
        final EmployeeRef employee;
        final String digest;

        Batch(List<String> rows, List<EmployeeRef> employees, EmployeeRef employee, String digest) {
            this.rows = rows;
            this.employees = employees;
            this.employee = employee;
            this.digest = digest;
        }

        static Batch rows(List<JsonNode> records) throws IOException {
            List<String> rows = new ArrayList<>(records.size());
            for (JsonNode record : records) {
                rows.add(BambooHRJson.asJson(record));
            }
            return new Batch(rows, null, null, BambooHRJson.digestRows(rows));
        }

        static Batch discovery(List<EmployeeRef> refs) {
            return new Batch(null, refs, null, BambooHRJson.digestEmployees(refs));
        }

        static Batch detail(String row, EmployeeRef employee) {
            return new Batch(List.of(row), null, employee, null);
        }
    }
}
