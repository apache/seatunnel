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

package org.apache.seatunnel.engine.server.event;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.event.EventHandler;
import org.apache.seatunnel.api.event.StainTraceEvent;
import org.apache.seatunnel.engine.server.trace.StainTracePayload;
import org.apache.seatunnel.engine.server.trace.StainTraceStage;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferProxy;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Writes StainTrace events to local JSONL files in OpenTelemetry OTLP JSON format.
 *
 * <p>Each line is a single {@code ExportTraceServiceRequest} (OTLP JSON) containing one span that
 * represents the end-to-end journey of one sampled row through the pipeline. Pipeline stages are
 * recorded as OTel Span Events with {@code timeUnixNano} timestamps.
 *
 * <p>OTLP JSON reference: https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding
 */
@Slf4j
public class JobEventLocalFileHandler implements EventHandler {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    public static final Duration REPORT_INTERVAL = Duration.ofSeconds(10);
    private static final int LOCAL_EVENT_BUFFER_CAPACITY = 2000;
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final String baseDir;
    private final int maxEventsPerFile;
    private final long maxFileSizeBytes;
    private final Ringbuffer ringbuffer;
    private volatile long committedEventIndex;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Object localBufferLock = new Object();
    private final Deque<Event> localBuffer = new ArrayDeque<>();

    private volatile TraceFileWriter currentWriter;
    private final Object writerLock = new Object();
    private final AtomicBoolean closing = new AtomicBoolean(false);

    public JobEventLocalFileHandler(String baseDir, Ringbuffer ringbuffer) {
        this(baseDir, REPORT_INTERVAL, ringbuffer, 10000, 10 * 1024 * 1024L);
    }

    public JobEventLocalFileHandler(
            String baseDir, Duration reportInterval, Ringbuffer ringbuffer) {
        this(baseDir, reportInterval, ringbuffer, 10000, 10 * 1024 * 1024L);
    }

    public JobEventLocalFileHandler(
            String baseDir,
            Duration reportInterval,
            Ringbuffer ringbuffer,
            int maxEventsPerFile,
            long maxFileSizeBytes) {
        this.baseDir = baseDir;
        this.maxEventsPerFile = maxEventsPerFile;
        this.maxFileSizeBytes = maxFileSizeBytes;
        this.ringbuffer = ringbuffer;
        this.committedEventIndex = ringbuffer.headSequence();
        this.scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder()
                                .setNameFormat("local-file-report-event-scheduler-%d")
                                .build());
        scheduledExecutorService.scheduleAtFixedRate(
                () -> {
                    if (closing.get()) {
                        return;
                    }
                    try {
                        report();
                    } catch (Throwable e) {
                        log.error("Failed to report event to local file", e);
                    }
                },
                0,
                reportInterval.getSeconds(),
                TimeUnit.SECONDS);
    }

    @Override
    public void handle(Event event) {
        if (closing.get()) {
            return;
        }
        addToLocalBuffer(event);
        try {
            ringbuffer.addAsync(event, OverflowPolicy.OVERWRITE);
        } catch (HazelcastInstanceNotActiveException e) {
            log.info("Skip writing event to ringbuffer because Hazelcast instance is not active");
        }
    }

    @VisibleForTesting
    /**
     * Flushes one batch from the ringbuffer so tests and shutdown hooks can reuse the same path.
     */
    synchronized void report() throws IOException {
        reportFromRingbuffer(false);
    }

    private boolean reportFromRingbuffer(boolean allowWriterCreateDuringClosing)
            throws IOException {
        long headSequence = ringbuffer.headSequence();
        if (headSequence > committedEventIndex) {
            log.warn(
                    "The head sequence {} is greater than the committed event index {}",
                    headSequence,
                    committedEventIndex);
            committedEventIndex = headSequence;
        }
        CompletionStage<ReadResultSet<Event>> completionStage =
                ringbuffer.readManyAsync(
                        committedEventIndex, 0, RingbufferProxy.MAX_BATCH_SIZE, null);
        ReadResultSet<Event> resultSet = completionStage.toCompletableFuture().join();
        if (resultSet.size() <= 0) {
            return false;
        }
        if (writeEventsToFile(resultSet, allowWriterCreateDuringClosing)) {
            committedEventIndex += resultSet.readCount();
            drainLocalBuffer(resultSet.readCount());
            return true;
        }
        return false;
    }

    private void reportFromLocalBuffer(boolean allowWriterCreateDuringClosing) throws IOException {
        List<Event> snapshot;
        synchronized (localBufferLock) {
            if (localBuffer.isEmpty()) {
                return;
            }
            snapshot = new ArrayList<>(localBuffer);
        }
        if (writeEventsToFile(snapshot, allowWriterCreateDuringClosing)) {
            synchronized (localBufferLock) {
                // Drain exactly the events we snapshotted.  Any events that arrived
                // between snapshot copy and this drain stay in the buffer for the next
                // flush, instead of being silently discarded by a full clear().
                int toDrain = snapshot.size();
                while (toDrain-- > 0 && !localBuffer.isEmpty()) {
                    localBuffer.pollFirst();
                }
            }
        }
    }

    /**
     * Converts buffered stain trace events into OTLP JSON lines and writes them to rotating files.
     */
    private boolean writeEventsToFile(
            Iterable<Event> events, boolean allowWriterCreateDuringClosing) throws IOException {
        // Guard before acquiring writerLock: if closing and this is not the authorized close-path
        // flush, bail out immediately.  A mid-loop return after partial writes would leave
        // committedEventIndex unchanged, causing close()'s re-read to duplicate those events.
        if (closing.get() && !allowWriterCreateDuringClosing) {
            return false;
        }
        synchronized (writerLock) {
            for (Event event : events) {
                if (!(event instanceof StainTraceEvent)) {
                    continue;
                }
                StainTraceEvent traceEvent = (StainTraceEvent) event;
                String jobId = traceEvent.getJobId();
                if (jobId == null || jobId.isEmpty()) {
                    log.warn("Skip event with null or empty jobId");
                    continue;
                }

                String otlpLine;
                try {
                    otlpLine = toOtlpJsonLine(traceEvent);
                } catch (Exception e) {
                    log.warn(
                            "Skip event with invalid payload for traceId={}: {}",
                            traceEvent.getTraceId(),
                            e.getMessage());
                    continue;
                }
                if (otlpLine == null) {
                    continue;
                }

                String date =
                        java.time.Instant.ofEpochMilli(traceEvent.getCreatedTime())
                                .atZone(ZoneId.systemDefault())
                                .toLocalDate()
                                .format(DATE_FORMATTER);

                if (currentWriter == null) {
                    currentWriter = new TraceFileWriter(baseDir, jobId, date);
                } else if (!jobId.equals(currentWriter.getJobId())
                        || !date.equals(currentWriter.getDate())) {
                    currentWriter.close();
                    currentWriter = new TraceFileWriter(baseDir, jobId, date);
                    log.info("Rotated trace file for job: {}", jobId);
                } else if (!closing.get()
                        && currentWriter.needsRotation(maxEventsPerFile, maxFileSizeBytes)) {
                    currentWriter.close();
                    currentWriter = new TraceFileWriter(baseDir, jobId, date);
                    log.info("Rotated trace file for job: {}", jobId);
                }
                currentWriter.writeJsonLine(otlpLine);
            }
            if (currentWriter != null) {
                currentWriter.flush();
            }
        }
        return true;
    }

    /**
     * Converts a StainTraceEvent to an OTLP JSON line (one ExportTraceServiceRequest per row).
     *
     * <p>OTel mapping:
     *
     * <ul>
     *   <li>One sampled row → one OTel Span
     *   <li>Each pipeline stage stamp → one Span Event with {@code timeUnixNano}
     *   <li>TraceId: 128-bit = 0x0000000000000000 || traceId (zero-extended from 64-bit)
     *   <li>SpanId: 64-bit = hex(sinkTaskId)
     * </ul>
     */
    private String toOtlpJsonLine(StainTraceEvent event) throws Exception {
        byte[] payload = event.getPayload();
        if (!StainTracePayload.isValid(payload)) {
            return null;
        }

        long traceId = StainTracePayload.readTraceId(payload);
        long startTsMs = StainTracePayload.readStartTsMs(payload);
        List<StainTracePayload.Entry> rawEntries = StainTracePayload.readEntries(payload);

        if (rawEntries.isEmpty()) {
            return null;
        }

        // OTel traceId: 128-bit hex (zero-extend our 64-bit id)
        String traceIdHex = String.format("%016x%016x", 0L, traceId);
        // OTel spanId: 64-bit hex from sinkTaskId
        String spanIdHex = String.format("%016x", event.getSinkTaskId());

        long endTsMs = startTsMs;
        List<Map<String, Object>> otlpEvents = new ArrayList<>();
        for (StainTracePayload.Entry entry : rawEntries) {
            if (entry.tsMs > endTsMs) {
                endTsMs = entry.tsMs;
            }
            String stageName =
                    StainTraceStage.fromCode(entry.stageCode)
                            .map(Enum::name)
                            .orElse("STAGE_" + entry.stageCode);

            Map<String, Object> evt = new LinkedHashMap<>();
            evt.put("name", stageName);
            evt.put("timeUnixNano", String.valueOf(entry.tsMs * 1_000_000L));
            evt.put(
                    "attributes",
                    Arrays.asList(
                            otlpIntAttr("seatunnel.stage_code", entry.stageCode),
                            otlpIntAttr("seatunnel.task_id", entry.taskId)));
            otlpEvents.add(evt);
        }

        // Build Span attributes
        List<Map<String, Object>> spanAttrs =
                Arrays.asList(
                        otlpStrAttr("seatunnel.table_id", event.getTableId()),
                        otlpIntAttr("seatunnel.sink_task_id", event.getSinkTaskId()));

        // Build Span
        Map<String, Object> span = new LinkedHashMap<>();
        span.put("traceId", traceIdHex);
        span.put("spanId", spanIdHex);
        span.put("parentSpanId", "");
        span.put("name", "seatunnel.record");
        span.put("kind", 1); // SPAN_KIND_INTERNAL
        span.put("startTimeUnixNano", String.valueOf(startTsMs * 1_000_000L));
        span.put("endTimeUnixNano", String.valueOf(endTsMs * 1_000_000L));
        span.put("attributes", spanAttrs);
        span.put("events", otlpEvents);
        Map<String, Object> statusMap = new LinkedHashMap<>();
        statusMap.put("code", 1); // STATUS_CODE_OK
        span.put("status", statusMap);

        // Build Resource
        List<Map<String, Object>> resourceAttrs =
                Arrays.asList(
                        otlpStrAttr("service.name", "seatunnel"),
                        otlpStrAttr("seatunnel.job_id", event.getJobId()));

        // Build scope
        Map<String, Object> scopeMap = new LinkedHashMap<>();
        scopeMap.put("name", "seatunnel.stain_trace");

        Map<String, Object> scopeSpanMap = new LinkedHashMap<>();
        scopeSpanMap.put("scope", scopeMap);
        scopeSpanMap.put("spans", Collections.singletonList(span));

        Map<String, Object> resourceMap = new LinkedHashMap<>();
        resourceMap.put("attributes", resourceAttrs);

        Map<String, Object> resourceSpanMap = new LinkedHashMap<>();
        resourceSpanMap.put("resource", resourceMap);
        resourceSpanMap.put("scopeSpans", Collections.singletonList(scopeSpanMap));

        // Build OTLP ExportTraceServiceRequest
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("resourceSpans", Collections.singletonList(resourceSpanMap));

        return JSON_MAPPER.writeValueAsString(root);
    }

    // --- OTLP attribute helpers ---

    private static Map<String, Object> otlpStrAttr(String key, String value) {
        Map<String, Object> valueMap = new LinkedHashMap<>();
        valueMap.put("stringValue", value != null ? value : "");
        Map<String, Object> attr = new LinkedHashMap<>();
        attr.put("key", key);
        attr.put("value", valueMap);
        return attr;
    }

    private static Map<String, Object> otlpIntAttr(String key, long value) {
        Map<String, Object> valueMap = new LinkedHashMap<>();
        valueMap.put("intValue", String.valueOf(value));
        Map<String, Object> attr = new LinkedHashMap<>();
        attr.put("key", key);
        attr.put("value", valueMap);
        return attr;
    }

    // --- Infrastructure ---

    /** Stops the scheduler and performs a best-effort final flush to local trace files. */
    @Override
    public void close() {
        log.info("Close local file report handler");
        // Signal handle() and the scheduled lambda to stop accepting new work.
        closing.set(true);
        scheduledExecutorService.shutdown();
        boolean schedulerTerminated = false;
        try {
            // Wait for any in-flight scheduled report() to finish before we
            // call report() ourselves, so committedEventIndex and currentWriter
            // are not touched concurrently.
            schedulerTerminated = scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (schedulerTerminated) {
            // Scheduler has stopped cleanly; safe to call synchronized report().
            try {
                reportFromRingbuffer(true);
            } catch (HazelcastInstanceNotActiveException e) {
                // Hazelcast shutting down — drain from local buffer instead
            } catch (IOException e) {
                log.error("Failed to flush events on close", e);
            }
        } else {
            // Scheduler did not stop within the timeout.  Interrupt it so any blocking
            // Hazelcast call (readManyAsync / CompletableFuture.join) is unblocked.
            scheduledExecutorService.shutdownNow();
            boolean finallyTerminated = false;
            try {
                finallyTerminated = scheduledExecutorService.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // Log how many ringbuffer events may have been skipped for observability.
            try {
                long tail = ringbuffer.tailSequence();
                long unsynced = tail - committedEventIndex + 1;
                if (unsynced > 0) {
                    log.warn(
                            "Scheduler timed out during close; up to {} ringbuffer event(s) were"
                                    + " not flushed to disk. Local buffer (cap={}) will be drained"
                                    + " as fallback.",
                            unsynced,
                            LOCAL_EVENT_BUFFER_CAPACITY);
                }
            } catch (Exception e) {
                log.warn(
                        "Scheduler timed out during close; relying on local buffer as fallback."
                                + " Could not determine dropped event count: {}",
                        e.getMessage());
            }
            if (!finallyTerminated) {
                // The scheduler thread is still alive even after shutdownNow().  Draining
                // the local buffer here would race with the scheduler on writerLock.
                // closing=true already prevents the scheduler from opening new files,
                // so we can safely close the current writer under the lock and return.
                log.warn(
                        "Scheduler thread did not terminate after shutdownNow();"
                                + " skipping local buffer drain to avoid concurrent writer access.");
                synchronized (writerLock) {
                    TraceFileWriter writer = currentWriter;
                    currentWriter = null;
                    if (writer != null) {
                        try {
                            writer.close();
                        } catch (IOException e) {
                            log.error("Failed to close current writer", e);
                        }
                    }
                }
                return;
            }
            try {
                reportFromRingbuffer(true);
            } catch (HazelcastInstanceNotActiveException e) {
                // Hazelcast shutting down — drain from local buffer instead
            } catch (IOException e) {
                log.error("Failed to flush events on close", e);
            }
        }
        // During Hazelcast/JVM shutdown the calling thread may have its interrupt
        // flag set (e.g. from an interrupted awaitTermination() above, or from the
        // engine shutdown machinery).  NIO writes check the interrupt flag and throw
        // ClosedByInterruptException, which also permanently closes the underlying
        // FileChannel.  Clear the flag before flushing so the write succeeds, then
        // restore it afterwards so callers still see the interrupted state.
        boolean wasInterrupted = Thread.interrupted();
        try {
            reportFromLocalBuffer(true);
        } catch (IOException e) {
            log.error("Failed to flush events from local buffer on close", e);
        } finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
        synchronized (writerLock) {
            TraceFileWriter writer = currentWriter;
            currentWriter = null;
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    log.error("Failed to close current writer", e);
                }
            }
        }
    }

    private void addToLocalBuffer(Event event) {
        synchronized (localBufferLock) {
            while (localBuffer.size() >= LOCAL_EVENT_BUFFER_CAPACITY) {
                localBuffer.pollFirst();
            }
            localBuffer.addLast(event);
        }
    }

    private void drainLocalBuffer(int count) {
        if (count <= 0) {
            return;
        }
        synchronized (localBufferLock) {
            int remaining = count;
            while (remaining > 0 && !localBuffer.isEmpty()) {
                localBuffer.pollFirst();
                remaining--;
            }
        }
    }

    @VisibleForTesting
    TraceFileWriter getCurrentWriter() {
        return currentWriter;
    }
}
