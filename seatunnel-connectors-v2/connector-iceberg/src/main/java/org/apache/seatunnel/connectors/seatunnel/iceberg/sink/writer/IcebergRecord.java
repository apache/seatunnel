/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer;

import org.apache.seatunnel.api.table.type.RowKind;

import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import java.util.Map;

public class IcebergRecord implements Record {

    private final Record delegate;
    private final RowKind rowKind;

    public IcebergRecord(Record delegate, RowKind rowKind) {
        this.delegate = delegate;
        this.rowKind = rowKind;
    }

    public RowKind getRowKind() {
        return rowKind;
    }

    @Override
    public Types.StructType struct() {
        return delegate.struct();
    }

    @Override
    public Object getField(String name) {
        return delegate.getField(name);
    }

    @Override
    public void setField(String name, Object value) {
        delegate.setField(name, value);
    }

    @Override
    public Object get(int pos) {
        return delegate.get(pos);
    }

    @Override
    public Record copy() {
        return new IcebergRecord(delegate.copy(), rowKind);
    }

    @Override
    public Record copy(Map<String, Object> overwriteValues) {
        return new IcebergRecord(delegate.copy(overwriteValues), rowKind);
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        return delegate.get(pos, javaClass);
    }

    @Override
    public <T> void set(int pos, T value) {
        delegate.set(pos, value);
    }
}
