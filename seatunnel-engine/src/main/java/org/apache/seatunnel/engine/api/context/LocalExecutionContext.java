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

package org.apache.seatunnel.engine.api.context;

import org.apache.seatunnel.engine.api.common.JobID;
import org.apache.seatunnel.engine.api.sink.Sink;
import org.apache.seatunnel.engine.api.source.Source;
import org.apache.seatunnel.engine.api.transform.AbstractTransformation;
import org.apache.seatunnel.engine.cache.CacheConfig;
import org.apache.seatunnel.engine.cache.DataStreamCache;
import org.apache.seatunnel.engine.config.Configuration;
import org.apache.seatunnel.engine.executionplan.JobInformation;
import org.apache.seatunnel.engine.logicalplan.DefaultLogicalPlan;
import org.apache.seatunnel.engine.logicalplan.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

public class LocalExecutionContext{
    private Source source;

    private List<AbstractTransformation> transformations;

    private Sink sink;

    private DataStreamCache dataStreamCache;

    private static String DEFAULT_JOB_NAME = "test_st_job";

    private Configuration configuration;

    private CacheConfig cacheConfig;

    private String jobName;

    private int maxParallelism = 1;

    public LocalExecutionContext(Configuration configuration) {
        this.configuration = configuration;
    }

    public void addTransformation(AbstractTransformation transformation) {
        if (transformations == null) {
            transformations = new ArrayList<AbstractTransformation>();
        }
        this.transformations.add(transformation);
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public List<AbstractTransformation> getTransformations() {
        return transformations;
    }

    public void setTransformations(List<AbstractTransformation> transformations) {
        this.transformations = transformations;
    }

    public Sink getSink() {
        return sink;
    }

    public void setSink(Sink sink) {
        this.sink = sink;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public LogicalPlan getLogicalPlan() {
        String jobName =  this.jobName == null ? DEFAULT_JOB_NAME : this.jobName;
        JobInformation jobInformation = new JobInformation(new JobID(), jobName, configuration, source.getBoundedness());
        DefaultLogicalPlan logicalPlan = new DefaultLogicalPlan(
                configuration,
                cacheConfig,
                jobInformation,
                this.source,
                this.transformations,
                this.sink,
                this.maxParallelism,
                this.dataStreamCache);
        logicalPlan.optimizer();
        return logicalPlan;
    }

    public void setDataStreamCache(DataStreamCache dataStreamCache) {
        this.dataStreamCache = dataStreamCache;
    }

    public void setCacheConfig(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    public void setMaxParallelism(int maxParallelism) {
        this.maxParallelism = maxParallelism;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}

