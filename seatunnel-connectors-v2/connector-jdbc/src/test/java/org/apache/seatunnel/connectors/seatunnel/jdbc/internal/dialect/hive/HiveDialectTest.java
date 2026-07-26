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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.hive;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

/** Verifies Hive metadata-query rewrite compatibility for simple SELECT and top-level CTE SQL. */
public class HiveDialectTest {

    /** Simple SELECT queries can be wrapped so metadata discovery reads at most one row. */
    @Test
    public void testModifySqlToLimit1WrapsSimpleSelect() throws Exception {
        Assertions.assertEquals(
                "SELECT * FROM (SELECT id FROM users) s LIMIT 1",
                modifySQLToLimit1("SELECT id FROM users;"));
    }

    /**
     * Top-level CTE queries must not be wrapped because Hive rejects WITH inside subquery blocks.
     */
    @Test
    public void testModifySqlToLimit1KeepsTopLevelWithQuery() throws Exception {
        String withQuery = "WITH t AS (SELECT 1 AS id) SELECT * FROM t";

        Assertions.assertEquals(withQuery, modifySQLToLimit1(withQuery));
    }

    /** Invokes the private rewrite helper without requiring a live Hive connection. */
    private String modifySQLToLimit1(String sql) throws Exception {
        Method method = HiveDialect.class.getDeclaredMethod("modifySQLToLimit1", String.class);
        method.setAccessible(true);
        return (String) method.invoke(new HiveDialect(), sql);
    }
}
