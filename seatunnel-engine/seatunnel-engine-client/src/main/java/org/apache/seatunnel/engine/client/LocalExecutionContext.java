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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.transform.Transformation;

import java.util.ArrayList;
import java.util.List;

public class LocalExecutionContext {
    private SeaTunnelSource source;

    private List<Transformation> transformations;

    private SeaTunnelSink sink;

    private static String DEFAULT_JOB_NAME = "test_st_job";

    private EngineClientConfig configuration;

    private String jobName;

    private int maxParallelism = 1;

    public LocalExecutionContext(EngineClientConfig configuration) {
        this.configuration = configuration;
    }

    public void addTransformation(Transformation transformation) {
        if (transformations == null) {
            transformations = new ArrayList<>();
        }
        this.transformations.add(transformation);
    }

    public SeaTunnelSource getSource() {
        return source;
    }

    public void setSource(SeaTunnelSource source) {
        this.source = source;
    }

    public List<Transformation> getTransformations() {
        return transformations;
    }

    public void setTransformations(List<Transformation> transformations) {
        this.transformations = transformations;
    }

    public SeaTunnelSink getSink() {
        return sink;
    }

    public void setSink(SeaTunnelSink sink) {
        this.sink = sink;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public void setMaxParallelism(int maxParallelism) {
        this.maxParallelism = maxParallelism;
    }
}

