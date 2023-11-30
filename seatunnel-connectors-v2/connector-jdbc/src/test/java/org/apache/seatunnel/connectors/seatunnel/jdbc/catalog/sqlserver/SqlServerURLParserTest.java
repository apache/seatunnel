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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver;

import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlServerURLParserTest {
    @Test
    public void testParse() {
        String url =
                "jdbc:sqlserver://localhost:1433;databaseName=myDB;encrypt=true;trustServerCertificate=false;loginTimeout=30;";
        JdbcUrlUtil.UrlInfo urlInfo = SqlServerURLParser.parse(url);
        assertEquals("localhost", urlInfo.getHost());
        assertEquals(1433, urlInfo.getPort());
        assertEquals(url, urlInfo.getOrigin());
        assertEquals(
                "encrypt=true;trustServerCertificate=false;loginTimeout=30", urlInfo.getSuffix());
        assertEquals("myDB", urlInfo.getDefaultDatabase().get());
        assertEquals(
                "jdbc:sqlserver://localhost:1433;encrypt=true;trustServerCertificate=false;loginTimeout=30",
                urlInfo.getUrlWithoutDatabase());
    }
}
