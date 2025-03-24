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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Converter that is responsible to convert between JDBC object and SeaTunnel data structure {@link
 * SeaTunnelRow}.
 */
public interface JdbcRowConverter extends Serializable {

    /**
     * Convert data retrieved from {@link ResultSet} to internal {@link SeaTunnelRow}.
     *
     * @param rs ResultSet from JDBC
     */
    SeaTunnelRow toInternal(ResultSet rs, TableSchema tableSchema) throws SQLException;

    @Deprecated
    PreparedStatement toExternal(
            TableSchema tableSchema, SeaTunnelRow row, PreparedStatement statement)
            throws SQLException;

    /** Convert data from internal {@link SeaTunnelRow} to JDBC object. */
    default PreparedStatement toExternal(
            TableSchema tableSchema,
            @Nullable TableSchema databaseTableSchema,
            SeaTunnelRow row,
            PreparedStatement statement)
            throws SQLException {
        return toExternal(tableSchema, row, statement);
    }
}
