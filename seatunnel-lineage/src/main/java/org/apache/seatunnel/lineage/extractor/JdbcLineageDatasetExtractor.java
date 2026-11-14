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

package org.apache.seatunnel.lineage.extractor;

import org.apache.seatunnel.lineage.LineageDataset;
import org.apache.seatunnel.lineage.LineageDatasetExtractor;
import org.apache.seatunnel.lineage.LineageDatasetNaming;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Extracts JDBC dataset identities from connector options. */
public final class JdbcLineageDatasetExtractor implements LineageDatasetExtractor {

    @Override
    public int order() {
        return 100;
    }

    @Override
    public boolean supports(String pluginName, Map<String, ?> options) {
        if (pluginName == null) {
            return false;
        }
        return LineageDatasetOptionSupport.string(
                                options, "url", "base-url", "jdbc_url", "jdbc-url")
                        != null
                || pluginName.toLowerCase().contains("jdbc")
                || pluginName.equalsIgnoreCase("mysql")
                || pluginName.equalsIgnoreCase("postgresql")
                || pluginName.equalsIgnoreCase("oracle");
    }

    @Override
    public List<LineageDataset> extract(Map<String, ?> options) {
        String url =
                LineageDatasetOptionSupport.string(
                        options, "url", "base-url", "jdbc_url", "jdbc-url");
        if (url == null || !url.startsWith("jdbc:")) {
            return Collections.emptyList();
        }
        try {
            URI uri = URI.create(url.substring("jdbc:".length()));
            String scheme = uri.getScheme();
            String host = uri.getHost();
            int port = uri.getPort();
            String urlDatabase = LineageDatasetOptionSupport.trimSlashes(uri.getPath());
            if (scheme == null || host == null || port <= 0) {
                return Collections.emptyList();
            }
            return LineageDatasetOptionSupport.datasets(
                    options,
                    reference ->
                            LineageDatasetNaming.jdbc(
                                    scheme,
                                    host,
                                    port,
                                    reference.database() == null
                                            ? urlDatabase
                                            : reference.database(),
                                    reference.table()));
        } catch (IllegalArgumentException e) {
            return Collections.emptyList();
        }
    }
}
