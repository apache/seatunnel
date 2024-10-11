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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.saphana;

import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SapHanaURLParser {

    private static final Pattern HANA_URL_PATTERN =
            Pattern.compile("^(?<url>jdbc:sap://(?<host>[^:]+):(?<port>\\d+)/\\?(?<params>.*?))$");

    public static JdbcUrlUtil.UrlInfo parse(String url) {
        Matcher matcher = HANA_URL_PATTERN.matcher(url);
        if (matcher.find()) {
            String urlWithoutDatabase = matcher.group("url");
            String host = matcher.group("host");
            Integer port = Integer.valueOf(matcher.group("port"));
            String params = matcher.group("params");
            return new JdbcUrlUtil.UrlInfo(url, urlWithoutDatabase, host, port, "SYSTEM", params);
        }
        return new JdbcUrlUtil.UrlInfo(url, url, null, null, "SYSTEM", null);
    }
}
