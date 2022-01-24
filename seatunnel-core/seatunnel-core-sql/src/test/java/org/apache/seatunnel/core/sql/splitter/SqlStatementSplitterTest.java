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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.util.List;

public class SqlStatementSplitterTest {

    @Test
    public void normalizeStatementsWithMultiSqls() {
        // this is bad case, multi sql should split by line \n
        String sqlContent = "--test is a comment \n select * from dual; select now(); select * from logs where log_content like ';'";
        List<String> sqls = SqlStatementSplitter.normalizeStatements(sqlContent);
        assertEquals(1, sqls.size());
        assertEquals("select * from dual; select now(); select * from logs where log_content like ';'", sqls.get(0));
    }

    @Test
    public void normalizeStatementsWithMultiLines() {
        String sqlContent = "--test is a comment \n select * from dual;\n select now();";
        List<String> sqls = SqlStatementSplitter.normalizeStatements(sqlContent);
        assertEquals(2, sqls.size());
        assertEquals("select * from dual", sqls.get(0));
        assertEquals("select now()", sqls.get(1));
    }

}
