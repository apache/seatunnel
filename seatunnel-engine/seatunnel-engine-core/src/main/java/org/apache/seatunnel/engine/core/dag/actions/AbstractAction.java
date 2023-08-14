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

package org.apache.seatunnel.engine.core.dag.actions;

import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;

import lombok.NonNull;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class AbstractAction implements Action {
    private String name;
    private transient List<Action> upstreams = new ArrayList<>();
    // This is used to assign a unique ID to every Action
    private long id;

    private int parallelism = 1;

    private final Set<URL> jarUrls;

    private final Config config;

    private final Set<ConnectorJarIdentifier> connectorJarIdentifiers;

    protected AbstractAction(
            long id,
            @NonNull String name,
            @NonNull Set<URL> jarUrls,
            @NonNull Set<ConnectorJarIdentifier> connectorJarIdentifiers) {
        this(id, name, new ArrayList<>(), jarUrls, connectorJarIdentifiers);
    }

    protected AbstractAction(
            long id,
            @NonNull String name,
            @NonNull List<Action> upstreams,
            @NonNull Set<URL> jarUrls,
            @NonNull Set<ConnectorJarIdentifier> connectorJarIdentifiers) {
        this(id, name, upstreams, jarUrls, connectorJarIdentifiers, null);
    }

    protected AbstractAction(
            long id,
            @NonNull String name,
            @NonNull List<Action> upstreams,
            @NonNull Set<URL> jarUrls,
            @NonNull Set<ConnectorJarIdentifier> connectorJarIdentifiers,
            Config config) {
        this.id = id;
        this.name = name;
        this.upstreams = upstreams;
        this.jarUrls = jarUrls;
        this.connectorJarIdentifiers = connectorJarIdentifiers;
        this.config = config;
    }

    @NonNull @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(@NonNull String name) {
        this.name = name;
    }

    @NonNull @Override
    public List<Action> getUpstream() {
        return upstreams;
    }

    @Override
    public void addUpstream(@NonNull Action action) {
        this.upstreams.add(action);
    }

    @Override
    public int getParallelism() {
        return parallelism;
    }

    @Override
    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public Set<URL> getJarUrls() {
        return jarUrls;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public Set<ConnectorJarIdentifier> getConnectorJarIdentifiers() {
        return connectorJarIdentifiers;
    }
}
