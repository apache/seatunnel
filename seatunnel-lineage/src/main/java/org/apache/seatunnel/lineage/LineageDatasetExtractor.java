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

package org.apache.seatunnel.lineage;

import java.util.List;
import java.util.Map;

/**
 * Pluggable extraction of canonical datasets from one connector option map.
 *
 * <p>Implementations are discovered through {@link java.util.ServiceLoader}. Built-in extractors
 * live in this module; connector-specific ones can be added later without growing {@link
 * LineageDatasetFactory}.
 */
public interface LineageDatasetExtractor {

    /**
     * Relative order among extractors. Lower values are tried first.
     *
     * <p>Built-ins use 10 (Paimon), 20 (Doris), and 100 (JDBC) so the JDBC heuristic remains a
     * fallback after named connector families.
     */
    default int order() {
        return 1000;
    }

    /** Returns whether this extractor handles the resolved plugin name and option map. */
    boolean supports(String pluginName, Map<String, ?> options);

    /**
     * Extracts canonical dataset identities from the connector option map.
     *
     * @param options connector options, without the enclosing plugin block
     */
    List<LineageDataset> extract(Map<String, ?> options);
}
