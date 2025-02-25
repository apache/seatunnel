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
import static org.junit.jupiter.api.Assertions.assertNull;

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

    @Test
    public void testParse2() {
        String url2 =
                "jdbc:sqlserver://localhost\\instanceName;databaseName=myDB;encrypt=true;trustServerCertificate=false;loginTimeout=30;";
        JdbcUrlUtil.UrlInfo urlInfo = SqlServerURLParser.parse(url2);
        assertEquals("localhost", urlInfo.getHost());
        assertNull(urlInfo.getPort());
        assertEquals(url2, urlInfo.getOrigin());
        assertEquals(
                "encrypt=true;trustServerCertificate=false;loginTimeout=30", urlInfo.getSuffix());
        assertEquals("myDB", urlInfo.getDefaultDatabase().get());
        assertEquals(
                "jdbc:sqlserver://localhost\\instanceName;encrypt=true;trustServerCertificate=false;loginTimeout=30",
                urlInfo.getUrlWithoutDatabase());
    }

    @Test
    public void testParse3() {
        String url3 =
                "jdbc:sqlserver://;serverName=localhost\\instanceName;databaseName=myDB;encrypt=true;trustServerCertificate=false;loginTimeout=30;";

        JdbcUrlUtil.UrlInfo urlInfo = SqlServerURLParser.parse(url3);
        assertEquals("localhost", urlInfo.getHost());
        assertNull(urlInfo.getPort());
        assertEquals(url3, urlInfo.getOrigin());
        assertEquals(
                "serverName=localhost\\instanceName;encrypt=true;trustServerCertificate=false;loginTimeout=30",
                urlInfo.getSuffix());
        assertEquals("myDB", urlInfo.getDefaultDatabase().get());
        assertEquals(
                "jdbc:sqlserver://localhost\\instanceName;serverName=localhost\\instanceName;encrypt=true;trustServerCertificate=false;loginTimeout=30",
                urlInfo.getUrlWithoutDatabase());
    }

    @Test
    public void testParse4() {
        String url4 =
                "jdbc:sqlserver://;serverName=localhost\\instanceName;port=1436;databaseName=myDB;encrypt=true;trustServerCertificate=false;loginTimeout=30;";

        JdbcUrlUtil.UrlInfo urlInfo = SqlServerURLParser.parse(url4);
        assertEquals("localhost", urlInfo.getHost());
        assertEquals(1436, urlInfo.getPort());
        assertEquals(url4, urlInfo.getOrigin());
        assertEquals(
                "serverName=localhost\\instanceName;port=1436;encrypt=true;trustServerCertificate=false;loginTimeout=30",
                urlInfo.getSuffix());
        assertEquals("myDB", urlInfo.getDefaultDatabase().get());
        assertEquals(
                "jdbc:sqlserver://localhost:1436;serverName=localhost\\instanceName;port=1436;encrypt=true;trustServerCertificate=false;loginTimeout=30",
                urlInfo.getUrlWithoutDatabase());
    }

    @Test
    public void testParse5() {
        String url5 =
                "jdbc:sqlserver://localhost\\instanceName;port=1436;databaseName=myDB;encrypt=true;trustServerCertificate=false;loginTimeout=30;";

        JdbcUrlUtil.UrlInfo urlInfo = SqlServerURLParser.parse(url5);
        assertEquals("localhost", urlInfo.getHost());
        assertEquals(1436, urlInfo.getPort());
        assertEquals(url5, urlInfo.getOrigin());
        assertEquals(
                "port=1436;encrypt=true;trustServerCertificate=false;loginTimeout=30",
                urlInfo.getSuffix());
        assertEquals("myDB", urlInfo.getDefaultDatabase().get());
        assertEquals(
                "jdbc:sqlserver://localhost:1436;port=1436;encrypt=true;trustServerCertificate=false;loginTimeout=30",
                urlInfo.getUrlWithoutDatabase());
    }

    @Test
    public void testIgnoreCase() {
        String url =
                "jdbc:sqlserver://localhost;DataBAseNaME=myDB;trustServerCertificate=false;PortNumBer=999;loginTimeout=30;SERVERname=test;";
        JdbcUrlUtil.UrlInfo urlInfo = SqlServerURLParser.parse(url);
        assertEquals("myDB", urlInfo.getDefaultDatabase().get());
        assertEquals(
                "jdbc:sqlserver://localhost:999;trustServerCertificate=false;PortNumBer=999;loginTimeout=30;SERVERname=test",
                urlInfo.getUrlWithoutDatabase());
        assertEquals(
                "trustServerCertificate=false;PortNumBer=999;loginTimeout=30;SERVERname=test",
                urlInfo.getSuffix());
        assertEquals("localhost", urlInfo.getHost());
        assertEquals(999, urlInfo.getPort());
    }

    @Test
    public void testWithoutInstanceName() {
        String url = "jdbc:sqlserver://sqlserver;encrypt=false;";
        JdbcUrlUtil.UrlInfo urlInfo = SqlServerURLParser.parse(url);
        assertEquals("sqlserver", urlInfo.getHost());
        assertEquals(1433, urlInfo.getPort());
        assertEquals(
                "jdbc:sqlserver://sqlserver:1433;encrypt=false", urlInfo.getUrlWithoutDatabase());
    }
}
