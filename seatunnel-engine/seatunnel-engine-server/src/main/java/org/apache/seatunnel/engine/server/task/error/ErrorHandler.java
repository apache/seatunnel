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

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/** Error handler for row-level error counting, logging and threshold checks. */
@Slf4j
public class ErrorHandler<T> implements Serializable, AutoCloseable {

    private final StageErrorConfig config;
    private final ErrorSinkRowWriter<T> errorSinkWriter;

    private final AtomicLong totalRecords = new AtomicLong(0);
    private final AtomicLong errorRecords = new AtomicLong(0);

    public ErrorHandler(StageErrorConfig config) {
        this(config, null);
    }

    public ErrorHandler(StageErrorConfig config, ErrorSinkRowWriter<T> errorSinkWriter) {
        this.config = config;
        this.errorSinkWriter = errorSinkWriter;
    }

    public void incrementTotalRecords() {
        if (config.getMode() == ErrorHandlerMode.DISABLE) {
            return;
        }
        totalRecords.incrementAndGet();
    }

    public void onError(RowErrorContext ctx, T row, Throwable t) {
        if (config.getMode() == ErrorHandlerMode.DISABLE) {
            return;
        }

        Objects.requireNonNull(ctx, "RowErrorContext must not be null");

        long currentErrorCount = errorRecords.incrementAndGet();

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
                            "Row-level error in stage [{}], plugin [{}] on table [{}]: {}. TotalRecords={}, ErrorRecords={}, Original data: {}",
                            stage,
                            pluginName,
                            tableId,
                            errorMessage,
                            totalRecords.get(),
                            currentErrorCount,
                            originalData,
                            t);
                } else {
                    log.warn(
                            "Row-level error in stage [{}], plugin [{}] on table [{}]: {}. TotalRecords={}, ErrorRecords={}, Original data: {}",
                            stage,
                            pluginName,
                            tableId,
                            errorMessage,
                            totalRecords.get(),
                            currentErrorCount,
                            originalData);
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
                errorSinkWriter.write(ctx, row, t);
            } catch (Exception sinkEx) {
                log.error(
                        "Error sink failed for stage [{}], plugin [{}], failing the job",
                        ctx.getStage(),
                        ctx.getPluginName(),
                        sinkEx);
                throw new RuntimeException(
                        String.format(
                                "Error sink failed for stage [%s], plugin [%s]",
                                ctx.getStage(), ctx.getPluginName()),
                        sinkEx);
            }
        }

        maybeThrowOnThreshold(ctx, currentErrorCount);
    }

    private void maybeThrowOnThreshold(RowErrorContext ctx, long currentErrorCount) {
        long total = totalRecords.get();
        if (config.getMaxErrorRecords() > 0 && currentErrorCount > config.getMaxErrorRecords()) {
            throw new RuntimeException(
                    String.format(
                            "Too many row-level errors in stage [%s], plugin [%s]: %d records exceeded max_error_records=%d",
                            ctx.getStage(),
                            ctx.getPluginName(),
                            currentErrorCount,
                            config.getMaxErrorRecords()));
        }

        // Only check ratio after min records threshold to ensure stability.
        int minTotalForRatio =
                config.getMaxErrorRatioMinRecords() > 0 ? config.getMaxErrorRatioMinRecords() : 1;
        if (config.getMaxErrorRatio() > 0 && total >= minTotalForRatio) {
            double ratio = (double) currentErrorCount / (double) total;
            if (ratio > config.getMaxErrorRatio()) {
                throw new RuntimeException(
                        String.format(
                                "Row-level error ratio in stage [%s], plugin [%s] exceeded max_error_ratio=%.4f (current=%.4f, errors=%d, total=%d)",
                                ctx.getStage(),
                                ctx.getPluginName(),
                                config.getMaxErrorRatio(),
                                ratio,
                                currentErrorCount,
                                total));
            }
        }
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

    public void flush() throws Exception {
        if (errorSinkWriter != null) {
            errorSinkWriter.flush();
        }
    }

    @Override
    public void close() {
        if (config.getMode() != ErrorHandlerMode.DISABLE) {
            log.info(
                    "ErrorHandler summary: mode={}, totalRecords={}, errorRecords={}, errorSinkEnabled={}",
                    config.getMode(),
                    totalRecords.get(),
                    errorRecords.get(),
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
