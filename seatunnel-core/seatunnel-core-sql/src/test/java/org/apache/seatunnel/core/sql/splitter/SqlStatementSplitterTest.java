/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.core.sql.splitter;

import java.util.List;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SqlStatementSplitterTest {

    @Test
    public void normalizeStatementsWithMultiSqls() {
        String sqlContent = "--test is a comment \n select * from dual; select now();";
        List<String> sqls = SqlStatementSplitter.normalizeStatements(sqlContent);
        assertThat(sqls.size(), is(2));
        assertThat(sqls.get(0), is("select * from dual"));
        assertThat(sqls.get(1), is("select now()"));
    }

    @Test
    public void normalizeStatementsWithMultiLines() {
        String sqlContent = "--test is a comment \n select * from dual;\n select now();";
        List<String> sqls = SqlStatementSplitter.normalizeStatements(sqlContent);
        assertThat(sqls.size(), is(2));
        assertThat(sqls.get(0), is("select * from dual"));
        assertThat(sqls.get(1), is("select now()"));
    }

}