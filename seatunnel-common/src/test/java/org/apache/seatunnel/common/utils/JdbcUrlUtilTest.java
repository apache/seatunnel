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

package org.apache.seatunnel.common.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcUrlUtilTest {

    @Test
    public void testMySQLUrlWithDatabase() {
        JdbcUrlUtil.UrlInfo urlInfo =
                JdbcUrlUtil.getUrlInfo("jdbc:mysql://192.168.1.1:5310/seatunnel?useSSL=true");
        Assertions.assertTrue(urlInfo.getUrlWithDatabase().isPresent());
        Assertions.assertTrue(urlInfo.getDefaultDatabase().isPresent());
        Assertions.assertEquals("seatunnel", urlInfo.getDefaultDatabase().get());
        Assertions.assertEquals(
                "jdbc:mysql://192.168.1.1:5310/seatunnel?useSSL=true",
                urlInfo.getUrlWithDatabase().get());
        Assertions.assertEquals("jdbc:mysql://192.168.1.1:5310", urlInfo.getUrlWithoutDatabase());
        Assertions.assertEquals("192.168.1.1", urlInfo.getHost());
        Assertions.assertEquals(5310, urlInfo.getPort());
        Assertions.assertEquals(
                urlInfo,
                JdbcUrlUtil.getUrlInfo("jdbc:mysql://192.168.1.1:5310/seatunnel?useSSL=true"));
    }

    @Test
    public void testMySQLUrlWithoutDatabase() {
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo("jdbc:mysql://192.168.1.1:5310/");
        Assertions.assertFalse(urlInfo.getUrlWithDatabase().isPresent());
        Assertions.assertFalse(urlInfo.getDefaultDatabase().isPresent());
        Assertions.assertEquals("jdbc:mysql://192.168.1.1:5310", urlInfo.getUrlWithoutDatabase());
        Assertions.assertEquals("192.168.1.1", urlInfo.getHost());
        Assertions.assertEquals(5310, urlInfo.getPort());
        Assertions.assertEquals(urlInfo, JdbcUrlUtil.getUrlInfo("jdbc:mysql://192.168.1.1:5310/"));
    }

    @Test
    public void testDuckDBUrl() {
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo("jdbc:duckdb:/tmp/seatunnel_test.db");
        Assertions.assertTrue(urlInfo.getUrlWithDatabase().isPresent());
        Assertions.assertTrue(urlInfo.getDefaultDatabase().isPresent());
        Assertions.assertEquals("tmp/seatunnel_test.db", urlInfo.getDefaultDatabase().get());
        Assertions.assertEquals("jdbc:duckdb:", urlInfo.getUrlWithoutDatabase());
        Assertions.assertEquals("localhost", urlInfo.getHost());
        Assertions.assertEquals(0, urlInfo.getPort());
        Assertions.assertEquals("", urlInfo.getSuffix());
        Assertions.assertEquals(
                "jdbc:duckdb:/tmp/seatunnel_test.db", urlInfo.getUrlWithDatabase().get());
        log.info("DuckDB URL parsed successfully: {}", urlInfo);
    }

    @Test
    public void testDuckDBUrlWithParameters() {
        JdbcUrlUtil.UrlInfo urlInfo =
                JdbcUrlUtil.getUrlInfo("jdbc:duckdb:/tmp/test.db?read_only=false");
        Assertions.assertTrue(urlInfo.getUrlWithDatabase().isPresent());
        Assertions.assertTrue(urlInfo.getDefaultDatabase().isPresent());
        Assertions.assertEquals("tmp/test.db", urlInfo.getDefaultDatabase().get());
        Assertions.assertEquals("jdbc:duckdb:", urlInfo.getUrlWithoutDatabase());
        Assertions.assertEquals("localhost", urlInfo.getHost());
        Assertions.assertEquals(0, urlInfo.getPort());
        Assertions.assertEquals("?read_only=false", urlInfo.getSuffix());
        Assertions.assertEquals(
                "jdbc:duckdb:/tmp/test.db?read_only=false", urlInfo.getUrlWithDatabase().get());
        log.info("DuckDB URL with parameters parsed successfully: {}", urlInfo);
    }

    @Test
    public void testSQLiteUrl() {
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo("jdbc:sqlite:/path/to/database.db");
        Assertions.assertTrue(urlInfo.getUrlWithDatabase().isPresent());
        Assertions.assertTrue(urlInfo.getDefaultDatabase().isPresent());
        Assertions.assertEquals("path/to/database.db", urlInfo.getDefaultDatabase().get());
        Assertions.assertEquals("jdbc:sqlite:", urlInfo.getUrlWithoutDatabase());
        Assertions.assertEquals("localhost", urlInfo.getHost());
        Assertions.assertEquals(0, urlInfo.getPort());
        Assertions.assertEquals(
                "jdbc:sqlite:/path/to/database.db", urlInfo.getUrlWithDatabase().get());
        log.info("SQLite URL parsed successfully: {}", urlInfo);
    }

    @Test
    public void testDuckDBInMemory() {
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo("jdbc:duckdb:");
        Assertions.assertTrue(urlInfo.getUrlWithDatabase().isPresent());
        Assertions.assertTrue(urlInfo.getDefaultDatabase().isPresent());
        Assertions.assertEquals("", urlInfo.getDefaultDatabase().get());
        Assertions.assertEquals("jdbc:duckdb:", urlInfo.getUrlWithoutDatabase());
        Assertions.assertEquals("localhost", urlInfo.getHost());
        Assertions.assertEquals(0, urlInfo.getPort());
        log.info("DuckDB in-memory URL parsed successfully: {}", urlInfo);
    }

    @Test
    public void testDuckDBAbsolutePath() {
        JdbcUrlUtil.UrlInfo urlInfo =
                JdbcUrlUtil.getUrlInfo("jdbc:duckdb:/absolute/path/to/test.db");
        Assertions.assertTrue(urlInfo.getUrlWithDatabase().isPresent());
        Assertions.assertTrue(urlInfo.getDefaultDatabase().isPresent());
        Assertions.assertEquals("absolute/path/to/test.db", urlInfo.getDefaultDatabase().get());
        Assertions.assertEquals("jdbc:duckdb:", urlInfo.getUrlWithoutDatabase());
        Assertions.assertEquals("localhost", urlInfo.getHost());
        Assertions.assertEquals(0, urlInfo.getPort());
        Assertions.assertEquals(
                "jdbc:duckdb:/absolute/path/to/test.db", urlInfo.getUrlWithDatabase().get());
        log.info("DuckDB absolute path URL parsed successfully: {}", urlInfo);
    }
}
