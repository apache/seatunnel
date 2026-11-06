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

import org.apache.seatunnel.lineage.extractor.LineageDatasetOptionSupport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Builds canonical datasets from connector option maps without depending on connector classes. */
public final class LineageDatasetFactory {

    private static final Logger LOGGER = Logger.getLogger(LineageDatasetFactory.class.getName());

    private static final List<LineageDatasetExtractor> EXTRACTORS = loadExtractors();

    private LineageDatasetFactory() {}

    /**
     * Extracts canonical dataset identities from one connector option map.
     *
     * <p>Only connector options that provide a real table identity are returned. Placeholder {@code
     * default.default.default} paths are excluded.
     *
     * @param options connector options, without the enclosing plugin block
     */
    public static List<LineageDataset> fromConnectorOptions(Map<String, ?> options) {
        return fromConnectorOptions(null, options);
    }

    /**
     * Extracts canonical dataset identities for a connector whose plugin name is already known.
     *
     * <p>The plugin name must be supplied by the caller whenever it has one. SeaTunnel jobs
     * normally declare a connector as a named block ({@code sink { Paimon { ... } }}), and the
     * option map for that block does not contain a {@code plugin_name} entry at all, so inferring
     * the name from the options alone silently yields no lineage for the most common job syntax.
     *
     * @param pluginName connector plugin name, or {@code null} to infer it from the options
     * @param options connector options, without the enclosing plugin block
     */
    public static List<LineageDataset> fromConnectorOptions(
            String pluginName, Map<String, ?> options) {
        if (options == null || options.isEmpty()) {
            return Collections.emptyList();
        }
        if (pluginName == null || pluginName.trim().isEmpty()) {
            pluginName = LineageDatasetOptionSupport.string(options, "plugin_name", "plugin-name");
        }
        if (pluginName == null) {
            pluginName = nestedPluginName(options);
        }
        if (pluginName == null) {
            return Collections.emptyList();
        }
        for (LineageDatasetExtractor extractor : EXTRACTORS) {
            if (extractor.supports(pluginName, options)) {
                return extractor.extract(options);
            }
        }
        return Collections.emptyList();
    }

    private static String nestedPluginName(Map<String, ?> options) {
        for (Map.Entry<String, ?> entry : options.entrySet()) {
            if (entry.getValue() instanceof Map) {
                String name =
                        LineageDatasetOptionSupport.string(
                                (Map<String, ?>) entry.getValue(), "plugin_name", "plugin-name");
                if (name != null) {
                    return name;
                }
                if ("paimon".equalsIgnoreCase(entry.getKey())
                        || "doris".equalsIgnoreCase(entry.getKey())) {
                    return entry.getKey();
                }
            }
        }
        return null;
    }

    /**
     * Discovers extractors with this class's own loader and skips broken providers.
     *
     * <p>Using {@link Class#getClassLoader()} rather than the thread context class loader keeps a
     * job class loader from being pinned in the static {@link #EXTRACTORS} list. Built-in
     * extractors are therefore discovered only via this module's class loader; third-party
     * extractors visible only on a job class loader are intentionally not loaded here (Option B /
     * per-loader caching would be needed later if that becomes a requirement). Iterating with an
     * explicit {@link Iterator} lets a single {@link ServiceConfigurationError} be logged and
     * skipped so one broken provider cannot fail class initialization for the whole factory.
     */
    private static List<LineageDatasetExtractor> loadExtractors() {
        ClassLoader loader = LineageDatasetFactory.class.getClassLoader();
        List<LineageDatasetExtractor> extractors = new ArrayList<>();
        Iterator<LineageDatasetExtractor> iterator =
                ServiceLoader.load(LineageDatasetExtractor.class, loader).iterator();
        while (true) {
            final boolean hasNext;
            try {
                hasNext = iterator.hasNext();
            } catch (ServiceConfigurationError e) {
                LOGGER.log(
                        Level.WARNING,
                        "Stopping LineageDatasetExtractor discovery after a broken provider lookup",
                        e);
                break;
            }
            if (!hasNext) {
                break;
            }
            try {
                extractors.add(iterator.next());
            } catch (ServiceConfigurationError e) {
                LOGGER.log(Level.WARNING, "Skipping a broken LineageDatasetExtractor provider", e);
            }
        }
        extractors.sort(Comparator.comparingInt(LineageDatasetExtractor::order));
        return Collections.unmodifiableList(extractors);
    }
}
