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

package org.apache.seatunnel.connectors.seatunnel.pulsar.source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

public class TableIdInjectingCollector<T> implements Collector<T> {
    private final Collector<T> delegate;
    private final String tableId;

    public TableIdInjectingCollector(Collector<T> delegate, TablePath tablePath) {
        this.delegate = delegate;
        this.tableId = tablePath.toString();
    }

    @Override
    public void collect(T record) {
        if (record instanceof SeaTunnelRow) {
            ((SeaTunnelRow) record).setTableId(tableId);
        }
        delegate.collect(record);
    }

    @Override
    public Object getCheckpointLock() {
        return delegate.getCheckpointLock();
    }

    @Override
    public void collect(SchemaChangeEvent event) {
        delegate.collect(event);
    }

    @Override
    public void markSchemaChangeBeforeCheckpoint() {
        delegate.markSchemaChangeBeforeCheckpoint();
    }

    @Override
    public void markSchemaChangeAfterCheckpoint() {
        delegate.markSchemaChangeAfterCheckpoint();
    }

    @Override
    public boolean isEmptyThisPollNext() {
        return delegate.isEmptyThisPollNext();
    }

    @Override
    public void resetEmptyThisPollNext() {
        delegate.resetEmptyThisPollNext();
    }
}
