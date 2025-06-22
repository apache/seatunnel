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

package org.apache.seatunnel.connectors.cdc.base.utils;

import java.util.List;

public class WhereConditionClauseHook implements SqlPostHook {

    private final String whereConditionClause;

    public WhereConditionClauseHook(String whereConditionClause) {
        this.whereConditionClause = whereConditionClause;
    }

    @Override
    public String apply(String sql) {
        if (whereConditionClause != null) {
            return applyWhereConditionClause(sql, whereConditionClause);
        }
        return sql;
    }

    /**
     * Apply a WHERE condition clause to a SQL query by wrapping it in a subquery.
     *
     * @param sql The original SQL query
     * @param whereConditionClause The WHERE condition clause to apply
     * @return The modified SQL query with the WHERE condition applied
     */
    public static String applyWhereConditionClause(String sql, String whereConditionClause) {
        return applyWhereConditionClause(sql, whereConditionClause, null);
    }

    /**
     * Apply a WHERE condition clause to a SQL query by wrapping it in a subquery. Also supports
     * specifying a list of fields to select instead of using "*".
     *
     * @param sql The original SQL query
     * @param whereConditionClause The WHERE condition clause to apply
     * @param fields The list of fields to select, or null to select all fields
     * @return The modified SQL query with the WHERE condition applied
     */
    public static String applyWhereConditionClause(
            String sql, String whereConditionClause, List<String> fields) {
        if (whereConditionClause != null && !whereConditionClause.trim().isEmpty()) {
            whereConditionClause = whereConditionClause.trim();
            if (!whereConditionClause.toLowerCase().startsWith("where")) {
                whereConditionClause = "WHERE " + whereConditionClause;
            }

            String selectClause = "*";
            if (fields != null && !fields.isEmpty()) {
                selectClause = String.join(", ", fields);
            }

            return String.format(
                    "SELECT %s FROM (%s) tmp %s", selectClause, sql, whereConditionClause);
        }

        // If no WHERE condition, but fields are specified, still apply the field selection
        if (fields != null && !fields.isEmpty()) {
            String selectClause = String.join(", ", fields);
            return String.format("SELECT %s FROM (%s) tmp", selectClause, sql);
        }

        return sql;
    }
}
