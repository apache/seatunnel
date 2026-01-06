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
}
