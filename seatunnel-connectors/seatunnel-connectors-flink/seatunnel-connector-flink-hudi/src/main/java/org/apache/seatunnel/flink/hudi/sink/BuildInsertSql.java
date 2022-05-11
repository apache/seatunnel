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

package org.apache.seatunnel.flink.hudi.sink;

import org.apache.flink.util.CollectionUtil;
import org.apache.parquet.Strings;

import java.util.List;

public class BuildInsertSql {
    private StringBuilder sql;
    private static final String BLANK_SPACE = " ";
    private static final String INSERT_PREFIX = "insert";
    private static final String APPEND = "into";
    private static final String OVERWRITE = "overwrite";
    private static final String PARTITION = "partition";
    private static final String SELECT = "select";
    private static final String SELECT_ALL = "select *";
    private static final String FIELD_SEPARATE = ",";
    private static final String FROM = "from";

    private BuildInsertSql() {

    }

    public static BuildInsertSql builder() {
        return new BuildInsertSql();
    }

    public BuildInsertSql withAppend() {
        sql = new StringBuilder().append(INSERT_PREFIX).append(BLANK_SPACE).append(APPEND).append(BLANK_SPACE);
        return this;
    }

    public BuildInsertSql withOverwrite() {
        sql = new StringBuilder().append(INSERT_PREFIX).append(BLANK_SPACE).append(OVERWRITE).append(BLANK_SPACE);
        return this;
    }

    public BuildInsertSql withPartition() {
        throw new RuntimeException("Unsupported insert partition now.");
    }

    public BuildInsertSql withSinkTable(String tableName) {
        sql.append(tableName).append(BLANK_SPACE);
        return this;
    }

    public BuildInsertSql withSelect(List<String> fields) {
        if (CollectionUtil.isNullOrEmpty(fields)){
            sql.append(SELECT_ALL).append(BLANK_SPACE);
        } else {
            sql.append(SELECT).append(Strings.join(fields, FIELD_SEPARATE)).append(BLANK_SPACE);
        }
        return this;
    }

    public BuildInsertSql withFrom(String tableName) {
        sql.append(FROM).append(BLANK_SPACE).append(tableName);
        return this;
    }

    public String build() {
        return sql.toString();
    }
}
