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

package org.apache.seatunnel.common.parsing;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.util.List;

public class StatementSqlBuilderTest {

    @Test
    @SuppressWarnings("magicnumber")
    public void testParse() {
        String originalSql = "insert into test (id, username, password, age, email) " +
            "values (#{id}, #{username}, #{password}, #{age}, #{email})";
        StatementSqlBuilder statementSqlBuilder = new StatementSqlBuilder(originalSql);
        statementSqlBuilder.parse();

        assertEquals(5, statementSqlBuilder.getParameters().size());
        assertEquals("insert into test (id, username, password, age, email) values (?, ?, ?, ?, ?)",
            statementSqlBuilder.getSql());
    }

    @Test
    @SuppressWarnings("magicnumber")
    public void testCharacterEscape() {
        String originalSql1 = "insert into test (id, username, password, age, email) " +
            "values (#{id}, \\#{username}, #{password}, #{age}, #{email})";
        StatementSqlBuilder statementSqlBuilder1 = new StatementSqlBuilder(originalSql1);
        statementSqlBuilder1.parse();

        assertEquals(4, statementSqlBuilder1.getParameters().size());
        assertEquals("insert into test (id, username, password, age, email) values (?, #{username}, ?, ?, ?)",
            statementSqlBuilder1.getSql());

        String originalSql2 = "insert into test (id, username, password, age, email) " +
            "values (#{#{id\\}}, #{\\#{username}, #{password}, #{age}, #{email})";
        StatementSqlBuilder statementSqlBuilder2 = new StatementSqlBuilder(originalSql2);
        statementSqlBuilder2.parse();

        final List<String> parameters = statementSqlBuilder2.getParameters();
        assertEquals(5, parameters.size());
        assertThat(parameters, hasItem("#{id}"));
        assertThat(parameters, hasItem("\\#{username"));
        assertEquals("insert into test (id, username, password, age, email) values (?, ?, ?, ?, ?)",
            statementSqlBuilder2.getSql());
    }
}
