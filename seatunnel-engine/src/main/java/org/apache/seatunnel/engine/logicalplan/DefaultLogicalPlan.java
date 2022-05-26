/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.logicalplan;

import org.apache.seatunnel.engine.api.sink.Sink;
import org.apache.seatunnel.engine.api.source.Boundedness;
import org.apache.seatunnel.engine.api.source.Source;
import org.apache.seatunnel.engine.api.transform.AbstractTransformation;
import org.apache.seatunnel.engine.cache.CacheConfig;
import org.apache.seatunnel.engine.cache.DataStreamCache;
import org.apache.seatunnel.engine.config.Configuration;
import org.apache.seatunnel.engine.executionplan.JobInformation;

import java.util.ArrayList;
import java.util.List;

public class DefaultLogicalPlan implements LogicalPlan {
    private Configuration configuration;

    private CacheConfig cacheConfig;

    private JobInformation jobInformation;

    private Source source;

    private List<AbstractTransformation> transformations;

    private Sink sink;

    private int maxParallelism;

    private Boundedness boundedness;

    private DataStreamCache dataStreamCache;

    private List<LogicalTask> logicalTasks;

    public DefaultLogicalPlan(Configuration configuration, CacheConfig cacheConfig, JobInformation jobInformation, Source source, List<AbstractTransformation> transformations, Sink sink, int maxParallelism, DataStreamCache dataStreamCache) {
        this.configuration = configuration;
        this.cacheConfig = cacheConfig;
        this.jobInformation = jobInformation;
        this.source = source;
        this.transformations = transformations;
        this.sink = sink;
        this.maxParallelism = maxParallelism;
        this.boundedness = source.getBoundedness();
        this.dataStreamCache = dataStreamCache;
        this.logicalTasks = new ArrayList<>(2);

        if (dataStreamCache != null && Boundedness.BOUNDED.equals(source.getBoundedness())) {
            throw new RuntimeException("bounded job not support cache");
        }
    }

    public void optimizer() {
        if (dataStreamCache != null) {
            optimizeCacheOperator();
        } else {
            LogicalTask logicalTask = new LogicalTask(this, source, transformations, sink, cacheConfig);
            logicalTasks.add(logicalTask);
        }
    }

    private void optimizeCacheOperator() {
        LogicalTask beforeCacheTask = new LogicalTask(this, source, null, dataStreamCache.createCacheSink(cacheConfig), cacheConfig);
        LogicalTask afterCacheTask = new LogicalTask(this, dataStreamCache.createCacheSource(cacheConfig), transformations, sink, cacheConfig);
        logicalTasks.add(beforeCacheTask);
        logicalTasks.add(afterCacheTask);
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public JobInformation getJobInformation() {
        return jobInformation;
    }

    @Override
    public Source getSource() {
        return source;
    }

    @Override
    public List<AbstractTransformation> getTransformations() {
        return transformations;
    }

    @Override
    public Sink getSink() {
        return sink;
    }

    @Override
    public int getMaxParallelism() {
        return maxParallelism;
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public DataStreamCache getDataStreamCache() {
        return dataStreamCache;
    }

    @Override
    public List<LogicalTask> getLogicalTasks() {
        return this.logicalTasks;
    }
}
