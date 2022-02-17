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

public class InfluxDbSql {

    private static final String QUERY_TEMPLATE = "SELECT %s FROM %s WHERE 1=1";

    private String measurement;
    private List<String> fields;
    private String startDate;
    private String endDate;

    public InfluxDbSql(String measurement, List<String> fields) {
        this.measurement = measurement;
        this.fields = fields;
    }

    public InfluxDbSql(String measurement, List<String> fields, String startDate, String endDate) {
        this.measurement = measurement;
        this.fields = fields;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public String getMeasurement() {
        return measurement;
    }

    public void setMeasurement(String measurement) {
        this.measurement = measurement;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InfluxDbSql that = (InfluxDbSql) o;
        return Objects.equals(measurement, that.measurement) && Objects.equals(fields, that.fields) && Objects.equals(startDate, that.startDate) && Objects.equals(endDate, that.endDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(measurement, fields, startDate, endDate);
    }

    public String sql() {
        String columns = String.join(",", getFields());
        String sql = String.format(QUERY_TEMPLATE, columns, getMeasurement());
        if (startDate != null) {
            sql += " AND time >= '" + startDate + "'";
        }
        if (endDate != null) {
            sql += " AND time < '" + endDate + "'";
        }
        return sql;
    }
}
