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

package org.apache.seatunnel.api.table.type;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Objects;

public class SqlDateType<T> implements SeaTunnelDataType<T> {
    private static final long serialVersionUID = 2L;

    public static final SqlDateType<Date> SQL_DATE_TYPE = new SqlDateType<>(Date.class, SqlType.DATE);
    public static final SqlDateType<Time> SQL_TIME_TYPE = new SqlDateType<>(Time.class, SqlType.TIME);
    public static final SqlDateType<Timestamp> SQL_DATE_TIME_TYPE = new SqlDateType<>(Timestamp.class, SqlType.TIMESTAMP);

    private final Class<T> typeClass;
    private final SqlType sqlType;

    private SqlDateType(Class<T> typeClass, SqlType sqlType) {
        this.typeClass = typeClass;
        this.sqlType = sqlType;
    }

    @Override
    public Class<T> getTypeClass() {
        return typeClass;
    }

    @Override
    public SqlType getSqlType() {
        return this.sqlType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeClass);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof SqlDateType)) {
            return false;
        }
        SqlDateType<?> that = (SqlDateType<?>) obj;
        return Objects.equals(typeClass, that.typeClass);
    }

    @Override
    public String toString() {
        return sqlType.toString();
    }
}
