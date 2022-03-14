/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.flink.source;

import java.util.List;
import java.util.Objects;

public class IotDbSql {

    private static final String QUERY_TEMPLATE = "SELECT %s FROM %s";

    private String storage;
    private List<String> fields;
    private String startTimestamp;
    private String endTimestamp;

    public IotDbSql(String storage, List<String> fields) {
        this.storage = storage;
        this.fields = fields;
    }

    public IotDbSql(String storage, List<String> fields, String startTimestamp, String endTimestamp) {
        this.storage = storage;
        this.fields = fields;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    public String getStorage() {
        return storage;
    }

    public void setStorage(String storage) {
        this.storage = storage;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public String getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(String startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public String getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(String endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IotDbSql that = (IotDbSql) o;
        return Objects.equals(storage, that.storage) && Objects.equals(fields, that.fields) && Objects.equals(startTimestamp, that.startTimestamp) && Objects.equals(endTimestamp, that.endTimestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storage, fields, startTimestamp, endTimestamp);
    }

    public String sql() {
        String columns = String.join(",", getFields());
        String sql = String.format(QUERY_TEMPLATE, columns, getStorage());
        if (startTimestamp != null) {
            sql += " AND time >= '" + startTimestamp + "'";
        }
        if (endTimestamp != null) {
            sql += " AND time < '" + endTimestamp + "'";
        }
        return sql;
    }
}
