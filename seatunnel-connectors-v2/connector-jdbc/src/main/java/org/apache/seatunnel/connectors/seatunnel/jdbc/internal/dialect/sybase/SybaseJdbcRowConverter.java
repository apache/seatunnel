/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sybase;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcFieldTypeUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.Optional;

@Slf4j
public class SybaseJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String converterName() {
        return DatabaseIdentifier.SYBASE;
    }

    @Override
    protected LocalTime readTime(ResultSet rs, int resultSetIndex) throws SQLException {
        Timestamp sqlTime = JdbcFieldTypeUtils.getTimestamp(rs, resultSetIndex);
        return Optional.ofNullable(sqlTime)
                .map(e -> e.toLocalDateTime().toLocalTime())
                .orElse(null);
    }
}
