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

package org.apache.seatunnel.engine.core.job;

import org.apache.seatunnel.engine.core.dag.actions.Action;

import org.apache.commons.collections4.CollectionUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class JarUtils {

    public static void addCommonPluginJarsToAction(
            Action action,
            Set<URL> commonPluginJars,
            Set<ConnectorJarIdentifier> commonJarIdentifiers) {

        if (CollectionUtils.isNotEmpty(commonPluginJars)) {
            action.getJarUrls().addAll(commonPluginJars);
        }

        if (CollectionUtils.isNotEmpty(commonJarIdentifiers)) {
            action.getConnectorJarIdentifiers().addAll(commonJarIdentifiers);
        }

        List<Action> upstreams = action.getUpstream();
        if (CollectionUtils.isNotEmpty(upstreams)) {
            upstreams.forEach(
                    upstreamAction ->
                            addCommonPluginJarsToAction(
                                    upstreamAction, commonPluginJars, commonJarIdentifiers));
        }
    }

    public static Set<URL> getJarUrlsFromIdentifiers(
            Set<ConnectorJarIdentifier> connectorJarIdentifiers) {
        Set<URL> jarUrls = new HashSet<>();
        for (ConnectorJarIdentifier connectorJarIdentifier : connectorJarIdentifiers) {
            File storageFile = new File(connectorJarIdentifier.getStoragePath());
            try {
                jarUrls.add(storageFile.toURI().toURL());
            } catch (MalformedURLException e) {
                log.warn("Cannot get plugin URL: [{}]", storageFile);
            }
        }
        return jarUrls;
    }
}
