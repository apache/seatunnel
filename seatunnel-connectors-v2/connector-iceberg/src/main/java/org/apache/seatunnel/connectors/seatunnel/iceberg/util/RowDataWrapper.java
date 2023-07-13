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

package org.apache.seatunnel.connectors.seatunnel.iceberg.util;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;

public class RowDataWrapper implements StructLike {

    private final SeaTunnelRowType rowType;
    private SeaTunnelRow rowData = null;

    public RowDataWrapper(SeaTunnelRowType rowType, Types.StructType struct) {
        this.rowType = rowType;
    }

    public RowDataWrapper wrap(SeaTunnelRow data) {
        this.rowData = data;
        return this;
    }

    @Override
    public int size() {
        return rowType.getFieldTypes().length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        if (rowData.isNullAt(pos)) {
            return null;
        } else {
            return javaClass.cast(rowData.getField(pos));
        }
    }

    @Override
    public <T> void set(int pos, T value) {
        throw new UnsupportedOperationException(
                "Could not set a field in the RowDataWrapper because rowData is read-only");
    }
}
