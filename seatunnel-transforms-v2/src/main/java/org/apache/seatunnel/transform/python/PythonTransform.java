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

package org.apache.seatunnel.transform.python;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.exception.TransformException;

import lombok.NonNull;

/** Row transform that delegates programmable field enrichment to a persistent Python worker. */
public class PythonTransform extends MultipleFieldOutputTransform {

    public static final String PLUGIN_NAME = "Python";

    /** Immutable configuration parsed from the job definition. */
    private final PythonTransformConfig transformConfig;

    /** Output columns appended to the input schema. */
    private final Column[] outputColumns;

    /** Lazily created Python process bound to this transform instance. */
    private transient PythonProcessWorker processWorker;

    /** Prevents close-before-open and concurrent close from recreating a worker. */
    private transient boolean closed;

    /** Operations that already acquired the current worker and must leave before close returns. */
    private transient int activeWorkerCalls;

    /** Ensures every concurrent close caller observes the same completed teardown. */
    private transient boolean closeInProgress;

    /** Marks the shared terminal close result as available. */
    private transient boolean closeCompleted;

    /** Failure produced by the owner close call and replayed to concurrent callers. */
    private transient Throwable closeFailure;

    /** Worker replacement failure merged into a concurrent or later terminal close. */
    private transient Throwable workerLifecycleFailure;

    /**
     * Creates one Python transform for a single catalog table.
     *
     * @param inputCatalogTable source schema seen by this transform
     * @param transformConfig normalized transform configuration
     */
    public PythonTransform(
            @NonNull CatalogTable inputCatalogTable,
            @NonNull PythonTransformConfig transformConfig) {
        super(inputCatalogTable, transformConfig.getErrorHandleWay());
        this.transformConfig = transformConfig;
        this.outputColumns =
                transformConfig.getColumnConfigs().stream()
                        .map(PythonColumnConfig::getDestColumn)
                        .toArray(Column[]::new);
    }

    /**
     * Returns the transform name exposed in job configs.
     *
     * @return plugin name
     */
    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    /** Starts the external Python worker before the first row is processed. */
    @Override
    public void open() {
        PythonProcessWorker currentWorker = acquireProcessWorker();
        try {
            currentWorker.open();
        } finally {
            releaseProcessWorker();
        }
    }

    /** Shuts down the external worker and releases temporary script files. */
    @Override
    public void close() {
        PythonProcessWorker currentWorker = null;
        boolean closeOwner = false;
        boolean interrupted = false;
        synchronized (this) {
            while (closeInProgress && !closeCompleted) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (!closeCompleted) {
                closed = true;
                closeInProgress = true;
                closeOwner = true;
                currentWorker = processWorker;
                processWorker = null;
            }
        }

        if (!closeOwner) {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            rethrowCloseFailure(closeFailure);
            return;
        }

        Throwable currentCloseFailure = null;
        try {
            if (currentWorker != null) {
                currentWorker.close();
            }
        } catch (RuntimeException | Error e) {
            currentCloseFailure = e;
        } finally {
            awaitActiveWorkerCalls();
            synchronized (this) {
                currentCloseFailure = mergeFailures(currentCloseFailure, workerLifecycleFailure);
                closeFailure = currentCloseFailure;
                closeCompleted = true;
                closeInProgress = false;
                notifyAll();
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        rethrowCloseFailure(currentCloseFailure);
    }

    /**
     * Delegates row processing to the Python worker and returns only the appended fields.
     *
     * @param inputRow current input row accessor
     * @return output field values produced by Python
     */
    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        PythonProcessWorker currentWorker = acquireProcessWorker();
        try {
            return currentWorker.processRow(inputRow);
        } finally {
            releaseProcessWorker();
        }
    }

    /**
     * Drops the cached worker after schema changes so the next open rebuilds Python-side field
     * metadata and JSON converters against the new input table layout.
     *
     * @param event upstream schema change event
     * @return event forwarded downstream unchanged
     */
    @Override
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent event) {
        beginLifecycleCall();
        try {
            SchemaChangeEvent mappedEvent = super.mapSchemaChangeEvent(event);
            invalidateProcessWorker();
            return mappedEvent;
        } finally {
            releaseProcessWorker();
        }
    }

    /**
     * Replaces the cached worker when the engine refreshes this transform from upstream's new
     * produced catalog.
     *
     * @param inputCatalogTable latest upstream catalog table
     */
    @Override
    public void setInputCatalogTable(@NonNull CatalogTable inputCatalogTable) {
        beginLifecycleCall();
        try {
            super.setInputCatalogTable(inputCatalogTable);
            invalidateProcessWorker();
        } finally {
            releaseProcessWorker();
        }
    }

    /**
     * Returns the columns appended to the produced schema.
     *
     * @return output columns
     */
    @Override
    protected Column[] getOutputColumns() {
        return outputColumns;
    }

    /**
     * Creates the worker lazily so planning does not require a local Python runtime.
     *
     * @return worker bound to this transform instance
     */
    private PythonProcessWorker acquireProcessWorker() {
        synchronized (this) {
            ensureNotClosed();
            if (processWorker == null) {
                processWorker = new PythonProcessWorker(transformConfig, inputCatalogTable);
            }
            activeWorkerCalls++;
            return processWorker;
        }
    }

    /** Admits a schema lifecycle call into the same terminal barrier as row processing. */
    private void beginLifecycleCall() {
        synchronized (this) {
            ensureNotClosed();
            activeWorkerCalls++;
        }
    }

    /** Rejects operations admitted after terminal close wins the lifecycle lock. */
    private void ensureNotClosed() {
        if (closed) {
            throw new TransformException(
                    PythonTransformErrorCode.PYTHON_PROCESS_TERMINATED_ERROR,
                    "Python transform has been closed");
        }
    }

    /** Marks one worker call complete and releases a concurrent close barrier. */
    private void releaseProcessWorker() {
        synchronized (this) {
            activeWorkerCalls--;
            notifyAll();
        }
    }

    /**
     * Waits uninterruptibly for calls admitted before close to leave, then restores interruption.
     */
    private void awaitActiveWorkerCalls() {
        boolean interrupted = false;
        synchronized (this) {
            while (activeWorkerCalls > 0) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** Replays the shared close result without wrapping its original error type. */
    private void rethrowCloseFailure(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
    }

    /** Keeps the first lifecycle failure and preserves later cleanup failures as suppressed. */
    private Throwable mergeFailures(Throwable primary, Throwable additional) {
        if (primary == null) {
            return additional;
        }
        if (additional != null && additional != primary) {
            primary.addSuppressed(additional);
        }
        return primary;
    }

    /**
     * Closes and discards the current worker so the next row rebuilds it from fresh schema state.
     */
    private void invalidateProcessWorker() {
        PythonProcessWorker currentWorker;
        synchronized (this) {
            currentWorker = processWorker;
            processWorker = null;
        }
        if (currentWorker == null) {
            return;
        }
        Throwable invalidationFailure = null;
        try {
            currentWorker.close();
        } catch (RuntimeException | Error e) {
            invalidationFailure = e;
            throw e;
        } finally {
            synchronized (this) {
                workerLifecycleFailure = mergeFailures(workerLifecycleFailure, invalidationFailure);
            }
        }
    }
}
