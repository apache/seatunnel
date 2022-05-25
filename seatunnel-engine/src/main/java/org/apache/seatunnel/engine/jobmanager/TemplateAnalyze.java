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

package org.apache.seatunnel.engine.jobmanager;

import org.apache.seatunnel.engine.api.sink.Sink;
import org.apache.seatunnel.engine.api.source.Source;
import org.apache.seatunnel.engine.api.transform.Transformation;
import org.apache.seatunnel.engine.cache.DataStreamCache;

import java.util.List;

public class TemplateAnalyze {
    private String templateFile;

    public TemplateAnalyze(String templateFile) {
        this.templateFile = templateFile;
    }

    public Source analyzeSource() {
        return null;
    }

    public List<Transformation> analyzeTransformations() {
        return null;
    }

    public Sink analyzeSink() {
        return null;
    }

    public DataStreamCache analyzeCacheInformation() {
        return null;
    }
}
