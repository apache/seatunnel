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

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Extracts Doris dataset identities from connector options. */
public final class DorisLineageDatasetExtractor implements LineageDatasetExtractor {

    @Override
    public int order() {
        return 20;
    }

    @Override
    public boolean supports(String pluginName, Map<String, ?> options) {
        return pluginName != null && "doris".equalsIgnoreCase(pluginName);
    }

    @Override
    public List<LineageDataset> extract(Map<String, ?> options) {
        String host =
                LineageDatasetOptionSupport.firstHost(
                        LineageDatasetOptionSupport.string(
                                options, "fenodes", "fe_nodes", "fe-nodes"));
        Integer queryPort =
                LineageDatasetOptionSupport.integer(
                        options, 9030, "query-port", "query_port", "query.port");
        if (queryPort == null) {
            return Collections.emptyList();
        }
        return LineageDatasetOptionSupport.datasets(
                options,
                reference ->
                        LineageDatasetNaming.doris(
                                host, queryPort, reference.database(), reference.table()));
    }
}
