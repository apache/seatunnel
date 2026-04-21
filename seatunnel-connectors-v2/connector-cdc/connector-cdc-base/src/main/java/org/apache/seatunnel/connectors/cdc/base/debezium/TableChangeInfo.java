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

package org.apache.seatunnel.connectors.cdc.base.debezium;

import java.io.Serializable;
import java.util.Objects;

public class TableChangeInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum TableChangeType {
        CREATE,
        ALTER,
        DROP
    }

    private final Object tableId;
    private final TableChangeType changeType;
    private final byte[] serializedTableSchema;

    public TableChangeInfo(
            Object tableId, TableChangeType changeType, byte[] serializedTableSchema) {
        this.tableId = tableId;
        this.changeType = changeType;
        this.serializedTableSchema = serializedTableSchema;
    }

    public Object getTableId() {
        return tableId;
    }

    public TableChangeType getChangeType() {
        return changeType;
    }

    public byte[] getSerializedTableSchema() {
        return serializedTableSchema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableChangeInfo that = (TableChangeInfo) o;
        return Objects.equals(tableId, that.tableId) && changeType == that.changeType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, changeType);
    }

    @Override
    public String toString() {
        return "TableChangeInfo{"
                + "tableId="
                + tableId
                + ", changeType="
                + changeType
                + ", schemaSize="
                + (serializedTableSchema != null ? serializedTableSchema.length : 0)
                + '}';
    }
}
