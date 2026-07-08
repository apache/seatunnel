/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MysqlDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.singlestore.SingleStoreDialect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test for {@link JdbcDialectLoader} */
public class JdbcDialectLoaderTest {
    @Test
    public void shouldFindGenericDialect() throws Exception {
        JdbcDialect jdbcDialect = JdbcDialectLoader.load("jdbc:someting:", null, "");
        Assertions.assertInstanceOf(GenericDialect.class, jdbcDialect);
    }

    @Test
    public void shouldFindMysqlDialect() throws Exception {
        JdbcDialect jdbcDialect =
                JdbcDialectLoader.load("jdbc:mysql://localhost:3306/test", null, "");
        Assertions.assertInstanceOf(MysqlDialect.class, jdbcDialect);
    }

    @Test
    public void shouldFindSingleStoreDialect() throws Exception {
        JdbcDialect jdbcDialect =
                JdbcDialectLoader.load("jdbc:singlestore://localhost:3306/test", null, "");
        Assertions.assertInstanceOf(SingleStoreDialect.class, jdbcDialect);
    }

    @Test
    public void shouldFindSingleStoreDialectWithBracketedIpv6Url() throws Exception {
        JdbcDialect jdbcDialect =
                JdbcDialectLoader.load("jdbc:singlestore://[2001:db8::1]/test", null, "");
        Assertions.assertInstanceOf(SingleStoreDialect.class, jdbcDialect);
    }

    @Test
    public void shouldFindSingleStoreDialectByDialect() throws Exception {
        JdbcDialect jdbcDialect = JdbcDialectLoader.load("jdbc:other://host/db", "SingleStore", "");
        Assertions.assertInstanceOf(SingleStoreDialect.class, jdbcDialect);
    }

    @Test
    public void shouldRejectInvalidSingleStoreUrl() throws Exception {
        JdbcDialect jdbcDialect = JdbcDialectLoader.load("jdbc:singlestore://", null, "");
        Assertions.assertFalse(jdbcDialect instanceof SingleStoreDialect);
    }

    @Test
    public void shouldLoadSingleStoreDialectByDialectNameWithFieldIde() throws Exception {
        JdbcDialect jdbcDialect =
                JdbcDialectLoader.load("jdbc:unknown://host/db", "", "SingleStore", "LOWERCASE");
        Assertions.assertInstanceOf(SingleStoreDialect.class, jdbcDialect);
        Assertions.assertEquals("`col`", jdbcDialect.quoteIdentifier("COL"));
    }

    /** Test for {@link JdbcDialectLoader} for appointDialect */
    @Test
    public void shouldFindPostgresSQLDialectByDialect() throws Exception {
        JdbcDialect jdbcDialect =
                JdbcDialectLoader.load("error:errorurl://xxxxx:3306/test", "Postgres", "");
        Assertions.assertInstanceOf(PostgresDialect.class, jdbcDialect);
    }
}
