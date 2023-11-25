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

import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDagGenerator;
import org.apache.seatunnel.engine.core.parse.MultipleTableJobConfigParser;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractJobEnvironment {
    protected static ILogger LOGGER = null;

    protected final boolean isStartWithSavePoint;

    protected final List<Action> actions = new ArrayList<>();

    protected final Set<URL> jarUrls = new HashSet<>();

    protected final Set<ConnectorJarIdentifier> connectorJarIdentifiers = new HashSet<>();

    protected final JobConfig jobConfig;

    protected final IdGenerator idGenerator;

    protected final List<URL> commonPluginJars = new ArrayList<>();

    public AbstractJobEnvironment(JobConfig jobConfig, boolean isStartWithSavePoint) {
        LOGGER = Logger.getLogger(getClass().getName());
        this.jobConfig = jobConfig;
        this.isStartWithSavePoint = isStartWithSavePoint;
        this.idGenerator = new IdGenerator();
        this.commonPluginJars.addAll(searchPluginJars());
        this.commonPluginJars.addAll(
                new ArrayList<>(
                        Common.getThirdPartyJars(
                                        jobConfig
                                                .getEnvOptions()
                                                .getOrDefault(EnvCommonOptions.JARS.key(), "")
                                                .toString())
                                .stream()
                                .map(Path::toUri)
                                .map(
                                        uri -> {
                                            try {
                                                return uri.toURL();
                                            } catch (MalformedURLException e) {
                                                throw new SeaTunnelEngineException(
                                                        "the uri of jar illegal:" + uri, e);
                                            }
                                        })
                                .collect(Collectors.toList())));
        LOGGER.info("add common jar in plugins :" + commonPluginJars);
    }

    protected Set<URL> searchPluginJars() {
        try {
            if (Files.exists(Common.pluginRootDir())) {
                return new HashSet<>(FileUtils.searchJarFiles(Common.pluginRootDir()));
            }
        } catch (IOException | SeaTunnelEngineException e) {
            LOGGER.warning(
                    String.format("Can't search plugin jars in %s.", Common.pluginRootDir()), e);
        }
        return Collections.emptySet();
    }

    public static void addCommonPluginJarsToAction(
            Action action,
            Set<URL> commonPluginJars,
            Set<ConnectorJarIdentifier> commonJarIdentifiers) {
        action.getJarUrls().addAll(commonPluginJars);
        action.getConnectorJarIdentifiers().addAll(commonJarIdentifiers);
        if (!action.getUpstream().isEmpty()) {
            action.getUpstream()
                    .forEach(
                            upstreamAction -> {
                                addCommonPluginJarsToAction(
                                        upstreamAction, commonPluginJars, commonJarIdentifiers);
                            });
        }
    }

    public static Set<URL> getJarUrlsFromIdentifiers(
            Set<ConnectorJarIdentifier> connectorJarIdentifiers) {
        Set<URL> jarUrls = new HashSet<>();
        connectorJarIdentifiers.stream()
                .map(
                        connectorJarIdentifier -> {
                            File storageFile = new File(connectorJarIdentifier.getStoragePath());
                            try {
                                return Optional.of(storageFile.toURI().toURL());
                            } catch (MalformedURLException e) {
                                LOGGER.warning(
                                        String.format("Cannot get plugin URL: {%s}", storageFile));
                                return Optional.empty();
                            }
                        })
                .collect(Collectors.toList())
                .forEach(
                        optional -> {
                            if (optional.isPresent()) {
                                jarUrls.add((URL) optional.get());
                            }
                        });
        return jarUrls;
    }

    protected abstract MultipleTableJobConfigParser getJobConfigParser();

    protected LogicalDagGenerator getLogicalDagGenerator() {
        return new LogicalDagGenerator(actions, jobConfig, idGenerator);
    }

    protected abstract LogicalDag getLogicalDag();
}
