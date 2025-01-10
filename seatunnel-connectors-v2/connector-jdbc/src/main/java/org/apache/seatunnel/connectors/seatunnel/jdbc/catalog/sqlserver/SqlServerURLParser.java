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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class SqlServerURLParser {
    private static final int DEFAULT_PORT = 1433;

    public static JdbcUrlUtil.UrlInfo parse(String url) {
        String serverName = "";
        Integer port = null;
        String dbInstance = null;
        String instanceName = null;
        int hostIndex = url.indexOf("://");
        if (hostIndex <= 0) {
            return null;
        }

        Map<String, String> props = Collections.emptyMap();
        String[] split = url.split(";", 2);
        if (split.length > 1) {
            props = parseQueryParams(split[1], ";");
            Map<String, String> propsWithUpperCaseKey =
                    props.entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            e -> e.getKey().toUpperCase(), Map.Entry::getValue));
            serverName = propsWithUpperCaseKey.get("SERVERNAME");
            instanceName = propsWithUpperCaseKey.get("INSTANCENAME");
            dbInstance = propsWithUpperCaseKey.getOrDefault("DATABASENAME", props.get("DATABASE"));
            if (propsWithUpperCaseKey.containsKey("PORTNUMBER")
                    || propsWithUpperCaseKey.containsKey("PORT")) {
                String portNumber =
                        propsWithUpperCaseKey.get("PORTNUMBER") == null
                                ? propsWithUpperCaseKey.get("PORT")
                                : propsWithUpperCaseKey.get("PORTNUMBER");
                try {
                    port = Integer.parseInt(portNumber);
                } catch (NumberFormatException ignored) {
                }
            }
        }

        String urlServerName = split[0].substring(hostIndex + 3);
        if (!urlServerName.isEmpty()) {
            serverName = urlServerName;
        }

        int portLoc = serverName.indexOf(":");
        if (portLoc > 1) {
            port = Integer.parseInt(serverName.substring(portLoc + 1));
            serverName = serverName.substring(0, portLoc);
        }

        int instanceLoc = serverName.indexOf("\\");
        if (instanceLoc > 1) {
            final String[] splitForInstance = serverName.split("\\\\");
            serverName = splitForInstance[0];
            instanceName = splitForInstance[1];
        }

        if (serverName.isEmpty()) {
            return null;
        }

        String suffix =
                props.entrySet().stream()
                        .filter(
                                e ->
                                        !e.getKey().equalsIgnoreCase("databaseName")
                                                && !e.getKey().equalsIgnoreCase("database"))
                        .map(e -> e.getKey() + "=" + e.getValue())
                        .collect(Collectors.joining(";", "", ""));
        suffix = Optional.ofNullable(suffix).orElse("");

        String urlWithoutDatabase;
        if (port != null) {
            urlWithoutDatabase =
                    String.format("jdbc:sqlserver://%s:%s", serverName, port) + ";" + suffix;
        } else if (instanceName != null) {
            urlWithoutDatabase =
                    String.format("jdbc:sqlserver://%s\\%s", serverName, instanceName)
                            + ";"
                            + suffix;
        } else {
            port = DEFAULT_PORT;
            urlWithoutDatabase =
                    String.format("jdbc:sqlserver://%s:%s", serverName, port) + ";" + suffix;
        }

        return new JdbcUrlUtil.UrlInfo(
                url, urlWithoutDatabase, serverName, port, dbInstance, suffix);
    }

    private static Map<String, String> parseQueryParams(String query, String separator) {
        if (query == null || query.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> queryParams = new LinkedHashMap<>();
        String[] pairs = query.split(separator);
        for (String pair : pairs) {
            try {
                int idx = pair.indexOf("=");
                String key =
                        idx > 0
                                ? URLDecoder.decode(
                                        pair.substring(0, idx), StandardCharsets.UTF_8.name())
                                : pair;
                if (!queryParams.containsKey(key)) {
                    String value =
                            idx > 0 && pair.length() > idx + 1
                                    ? URLDecoder.decode(
                                            pair.substring(idx + 1), StandardCharsets.UTF_8.name())
                                    : null;
                    queryParams.put(key, value);
                }
            } catch (UnsupportedEncodingException e) {
                // Ignore.
            }
        }
        return queryParams;
    }
}
