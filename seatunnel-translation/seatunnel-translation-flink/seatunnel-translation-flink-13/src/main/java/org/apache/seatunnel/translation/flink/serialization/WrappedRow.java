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

package org.apache.seatunnel.translation.flink.serialization;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

/**
 * Wrapped {@link Row}.
 *
 * <p>Keep the original table name for the Dispatcher to distribute to the corresponding data stream
 */
public class WrappedRow extends Tuple2<Row, String> {
    private static final long serialVersionUID = -8325988931728734377L;

    public WrappedRow() {
        super();
    }

    public WrappedRow(Row row, String table) {
        super(row, table);
    }

    public Row getRow() {
        return this.f0;
    }

    public String getTable() {
        return this.f1;
    }

    public void setRow(Row row) {
        this.f0 = row;
    }

    public void setTable(String table) {
        this.f1 = table;
    }
}
