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

public class DruidSql {

    private static final String QUERY_TEMPLATE = "SELECT %s FROM %s WHERE 1=1";
    private static final String COLUMNS_DEFAULT = "*";

    private String datasource;
    private String startDate;
    private String endDate;
    private List<String> columns;

    public DruidSql(String datasource) {
        this.datasource = datasource;
    }

    public DruidSql(String datasource, String startDate, String endDate) {
        this.datasource = datasource;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public DruidSql(String datasource, String startDate, String endDate, List<String> columns) {
        this.datasource = datasource;
        this.startDate = startDate;
        this.endDate = endDate;
        this.columns = columns;
    }

    public String getDatasource() {
        return datasource;
    }

    public void setDatasource(String datasource) {
        this.datasource = datasource;
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

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DruidSql druidSql = (DruidSql) o;
        return Objects.equals(datasource, druidSql.datasource) && Objects.equals(startDate, druidSql.startDate) && Objects.equals(endDate, druidSql.endDate) && Objects.equals(columns, druidSql.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasource, startDate, endDate, columns);
    }

    public String sql() {
        String columns = COLUMNS_DEFAULT;
        if (getColumns() != null && getColumns().size() > 0) {
            columns = String.join(",", getColumns());
        }
        String sql = String.format(QUERY_TEMPLATE, columns, getDatasource());
        if (startDate != null) {
            sql += " AND __time >= TIMESTAMP '" + startDate + "'";
        }
        if (endDate != null) {
            sql += " AND __time < TIMESTAMP '" + endDate + "'";
        }
        return sql;
    }
}
