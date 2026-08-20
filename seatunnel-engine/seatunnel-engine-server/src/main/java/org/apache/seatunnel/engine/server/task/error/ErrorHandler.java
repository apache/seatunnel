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

package org.apache.seatunnel.engine.server.task.error;

import org.apache.seatunnel.api.common.error.RowErrorHandlingFatalException;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Objects;

/**
 * Error handler for row-level error counting, logging, routing, and threshold checks.
 *
 * <p>The handler keeps row-processing operations synchronous with the owning task. Checkpoint-aware
 * counters and error-sink flushes are exposed through explicit lifecycle methods so task flows can
 * bind them to SeaTunnel checkpoint completion instead of publishing partial state immediately.
 */
@Slf4j
public class ErrorHandler<T> implements Serializable, AutoCloseable {

    public enum ErrorHandleResult {
        DROPPED,
        ROUTED_TO_ERROR_SINK
    }

    private final StageErrorConfig config;
    private final ErrorSinkRowWriter<T> errorSinkWriter;
    private final ErrorHandlerCounter counter;

    public ErrorHandler(StageErrorConfig config) {
        this(config, null);
    }

    public ErrorHandler(StageErrorConfig config, ErrorSinkRowWriter<T> errorSinkWriter) {
        this(config, errorSinkWriter, new LocalErrorHandlerCounter());
    }

    public ErrorHandler(
            StageErrorConfig config,
            ErrorSinkRowWriter<T> errorSinkWriter,
            ErrorHandlerCounter counter) {
        this.config = config;
        this.errorSinkWriter = errorSinkWriter;
        this.counter = Objects.requireNonNull(counter, "counter");
    }

    /** Records a non-error input row for ratio/record thresholds. */
    public void incrementTotalRecords() {
        if (config.getMode() == ErrorHandlerMode.DISABLE) {
            return;
        }
        long currentTotal = counter.incrementTotalRecords();
        maybeThrowOnRatioThreshold(null, counter.getErrorRecords(), currentTotal);
    }

    public ErrorHandlerMode getMode() {
        return config.getMode();
    }

    /** Handles a row-level failure according to the configured mode and thresholds. */
    public ErrorHandleResult onError(RowErrorContext ctx, T row, Throwable t) {
        if (config.getMode() == ErrorHandlerMode.DISABLE) {
            return ErrorHandleResult.DROPPED;
        }

        long currentErrorCount = counter.incrementErrorRecords();
        ErrorHandleResult result = ErrorHandleResult.DROPPED;

        // Build original data safely.
        String originalData = null;
        if (config.isIncludeOriginalData()) {
            try {
                originalData = truncate(String.valueOf(row), config.getOriginalDataMaxLength());
            } catch (Throwable buildEx) {
                if (buildEx instanceof Error) {
                    throw (Error) buildEx;
                }
                log.error(
                        "Failed to build original_data for row-level error. stage={}, plugin={}, tableId={}, originalError={}",
                        ctx.getStage(),
                        ctx.getPluginName(),
                        ctx.getTableId(),
                        t != null ? t.getMessage() : null,
                        buildEx);
            }
        }

        // Log error when LOG/ROUTE mode is enabled.
        if (config.getMode() == ErrorHandlerMode.LOG
                || config.getMode() == ErrorHandlerMode.ROUTE) {
            try {
                String stage = ctx.getStage();
                String pluginName = ctx.getPluginName();
                String tableId = ctx.getTableId();
                String errorMessage = t != null ? t.getMessage() : null;

                if (config.isIncludeStacktrace() && t != null) {
                    log.warn(
                            "Row-level error in stage [{}], plugin [{}] on table [{}]: {}. TotalRecords={}, ErrorRecords={}",
                            stage,
                            pluginName,
                            tableId,
                            errorMessage,
                            counter.getTotalRecords(),
                            currentErrorCount,
                            t);
                } else {
                    log.warn(
                            "Row-level error in stage [{}], plugin [{}] on table [{}]: {}. TotalRecords={}, ErrorRecords={}",
                            stage,
                            pluginName,
                            tableId,
                            errorMessage,
                            counter.getTotalRecords(),
                            currentErrorCount);
                }
                if (originalData != null) {
                    log.debug(
                            "Original row data for row-level error is available only in routed error records. stage={}, plugin={}, tableId={}, originalDataLength={}",
                            stage,
                            pluginName,
                            tableId,
                            originalData.length());
                }
            } catch (Throwable logEx) {
                if (logEx instanceof Error) {
                    throw (Error) logEx;
                }
                log.error(
                        "Failed to log row-level error. stage={}, plugin={}, tableId={}, originalError={}, logFailure={}",
                        ctx.getStage(),
                        ctx.getPluginName(),
                        ctx.getTableId(),
                        t != null ? t.getMessage() : null,
                        logEx.getMessage(),
                        logEx);
            }
        }

        // In ROUTE mode, write to error sink. Failures will fail the job.
        if (config.getMode() == ErrorHandlerMode.ROUTE && errorSinkWriter != null) {
            try {
                log.debug(
                        "Writing error row to sink. stage={}, plugin={}, tableId={}",
                        ctx.getStage(),
                        ctx.getPluginName(),
                        ctx.getTableId());
                boolean accepted = errorSinkWriter.writeAndCheckAccepted(ctx, row, t);
                result =
                        accepted
                                ? ErrorHandleResult.ROUTED_TO_ERROR_SINK
                                : ErrorHandleResult.DROPPED;
            } catch (Exception sinkEx) {
                log.error(
                        "Error sink failed for stage [{}], plugin [{}], failing the job",
                        ctx.getStage(),
                        ctx.getPluginName(),
                        sinkEx);
                throw new RowErrorHandlingFatalException(
                        String.format(
                                "Error sink failed for stage [%s], plugin [%s]",
                                ctx.getStage(), ctx.getPluginName()),
                        sinkEx);
            }
        }

        maybeThrowOnThreshold(ctx, currentErrorCount);
        return result;
    }

    private void maybeThrowOnThreshold(RowErrorContext ctx, long currentErrorCount) {
        if (config.getMaxErrorRecords() > 0 && currentErrorCount > config.getMaxErrorRecords()) {
            throw new RowErrorHandlingFatalException(
                    String.format(
                            "Too many row-level errors in stage [%s], plugin [%s]: %d records exceeded max_error_records=%d",
                            stageName(ctx),
                            pluginName(ctx),
                            currentErrorCount,
                            config.getMaxErrorRecords()));
        }

        maybeThrowOnRatioThreshold(ctx, currentErrorCount, counter.getTotalRecords());
    }

    private void maybeThrowOnRatioThreshold(
            RowErrorContext ctx, long currentErrorCount, long total) {
        // Only check ratio after min records threshold to ensure stability.
        int minTotalForRatio =
                config.getMaxErrorRatioMinRecords() > 0 ? config.getMaxErrorRatioMinRecords() : 1;
        if (config.getMaxErrorRatio() > 0 && total >= minTotalForRatio) {
            double ratio = (double) currentErrorCount / (double) total;
            if (ratio > config.getMaxErrorRatio()) {
                throw new RowErrorHandlingFatalException(
                        String.format(
                                "Row-level error ratio in stage [%s], plugin [%s] exceeded max_error_ratio=%.4f (current=%.4f, errors=%d, total=%d)",
                                stageName(ctx),
                                pluginName(ctx),
                                config.getMaxErrorRatio(),
                                ratio,
                                currentErrorCount,
                                total));
            }
        }
    }

    private String stageName(RowErrorContext ctx) {
        return ctx == null ? "UNKNOWN" : ctx.getStage();
    }

    private String pluginName(RowErrorContext ctx) {
        return ctx == null ? "UNKNOWN" : ctx.getPluginName();
    }

    private String truncate(String value, int maxLength) {
        if (value == null) {
            return null;
        }
        if (maxLength <= 0) {
            return "";
        }
        if (value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }

    /** Flushes pending error-sink rows outside a checkpoint boundary. */
    public void flush() throws Exception {
        if (errorSinkWriter != null) {
            errorSinkWriter.flush();
        }
    }

    /** Flushes pending error-sink rows as part of the given checkpoint. */
    public void flush(long checkpointId) throws Exception {
        if (errorSinkWriter != null) {
            errorSinkWriter.flush(checkpointId);
        }
    }

    /** Captures local threshold-counter deltas for the checkpoint. */
    public void snapshotState(long checkpointId) {
        counter.snapshotState(checkpointId);
    }

    /** Publishes threshold-counter deltas captured for a completed checkpoint. */
    public void notifyCheckpointComplete(long checkpointId) {
        counter.notifyCheckpointComplete(checkpointId);
    }

    /** Drops threshold-counter deltas captured for an aborted checkpoint. */
    public void notifyCheckpointAborted(long checkpointId) {
        counter.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void close() {
        if (config.getMode() != ErrorHandlerMode.DISABLE) {
            log.info(
                    "ErrorHandler summary: mode={}, totalRecords={}, errorRecords={}, errorSinkEnabled={}",
                    config.getMode(),
                    counter.getTotalRecords(),
                    counter.getErrorRecords(),
                    errorSinkWriter != null);
        }
        if (errorSinkWriter != null) {
            try {
                errorSinkWriter.close();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Failed to close error sink writer", e);
            }
        }
    }
}
