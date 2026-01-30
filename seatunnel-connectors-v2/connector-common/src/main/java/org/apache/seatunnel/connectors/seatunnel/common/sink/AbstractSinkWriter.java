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

package org.apache.seatunnel.connectors.seatunnel.common.sink;

import org.apache.seatunnel.api.sink.DirtyRecordCollector;
import org.apache.seatunnel.api.sink.NoOpDirtyRecordCollector;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Optional;

@Slf4j
public abstract class AbstractSinkWriter<T, StateT> implements SinkWriter<T, Void, StateT> {

    protected SinkWriter.Context context;

    protected AbstractSinkWriter() {}

    protected AbstractSinkWriter(SinkWriter.Context context) {
        this.context = context;
    }

    /**
     * Template method that wraps {@link #doWrite(Object)} with dirty data collection.
     *
     * <p>Override this method directly if you want full control over write + dirty handling.
     * Override {@link #doWrite(Object)} if you only need to implement the write logic and want
     * automatic dirty collection.
     */
    @Override
    public void write(T element) throws IOException {
        if (validateDirtyRecord(element)) {
            return;
        }
        try {
            doWrite(element);
        } catch (Exception e) {
            if (!tryCollectDirtyRecord(element, e)) {
                if (e instanceof IOException) {
                    throw (IOException) e;
                }
                throw new IOException(e);
            }
        }
    }

    /**
     * Subclasses implement this method for actual write logic. Exceptions thrown here are
     * automatically caught and routed to the dirty data collector.
     *
     * @param element the data to write
     * @throws IOException if write fails and should be treated as dirty data
     */
    protected void doWrite(T element) throws IOException {
        throw new UnsupportedOperationException(
                "Subclass must override either write() or doWrite()");
    }

    /**
     * Pre-validates the record against user-defined dirty rules. Returns true if the record was
     * collected as dirty.
     */
    protected boolean validateDirtyRecord(T element) {
        if (context == null) {
            return false;
        }
        DirtyRecordCollector collector = context.getDirtyRecordCollector();
        if (collector == null || collector instanceof NoOpDirtyRecordCollector) {
            return false;
        }
        if (element instanceof SeaTunnelRow) {
            return collector.validateAndCollectIfDirty(
                    context.getIndexOfSubtask(), (SeaTunnelRow) element, null);
        }
        return false;
    }

    /**
     * Attempts to collect a failed record as dirty data. Returns true if the record was collected ,
     * false if it should be re-thrown.
     */
    protected boolean tryCollectDirtyRecord(T element, Exception e) {
        if (context == null) {
            return false;
        }
        DirtyRecordCollector collector = context.getDirtyRecordCollector();
        if (collector == null || collector instanceof NoOpDirtyRecordCollector) {
            return false;
        }
        if (element instanceof SeaTunnelRow) {
            collector.collect(
                    context.getIndexOfSubtask(),
                    (SeaTunnelRow) element,
                    e,
                    "Write failed: " + e.getMessage());
            return true;
        }
        return false;
    }

    @Override
    public Optional<Void> prepareCommit() {
        return Optional.empty();
    }

    public final void abortPrepare() {
        // nothing
    }
}
