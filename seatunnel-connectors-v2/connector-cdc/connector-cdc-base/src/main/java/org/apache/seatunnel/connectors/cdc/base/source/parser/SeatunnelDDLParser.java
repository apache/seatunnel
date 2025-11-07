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

package org.apache.seatunnel.connectors.cdc.base.source.parser;

import org.apache.seatunnel.api.table.catalog.TableIdentifier;

import org.apache.commons.lang3.StringUtils;

import io.debezium.relational.Column;
import io.debezium.relational.TableId;

public interface SeatunnelDDLParser {

    /**
     * @param column The column to convert
     * @return The converted column in SeaTunnel format which has full type information
     */
    default org.apache.seatunnel.api.table.catalog.Column toSeatunnelColumnWithFullTypeInfo(
            Column column) {
        org.apache.seatunnel.api.table.catalog.Column seatunnelColumn = toSeatunnelColumn(column);
        String sourceColumnType = getSourceColumnTypeWithLengthScale(column);
        return seatunnelColumn.reSourceType(sourceColumnType);
    }

    /**
     * @param column The column to convert
     * @return The converted column in SeaTunnel format
     */
    org.apache.seatunnel.api.table.catalog.Column toSeatunnelColumn(Column column);

    /**
     * @param column The column to convert
     * @return The type with length and scale
     */
    default String getSourceColumnTypeWithLengthScale(Column column) {
        StringBuilder sb = new StringBuilder(column.typeName());
        if (column.length() >= 0) {
            sb.append('(').append(column.length());
            if (column.scale().isPresent()) {
                sb.append(", ").append(column.scale().get());
            }

            sb.append(')');
        }
        return sb.toString();
    }

    default TableIdentifier toTableIdentifier(TableId tableId) {
        return new TableIdentifier(
                StringUtils.EMPTY, tableId.catalog(), tableId.schema(), tableId.table());
    }
}
