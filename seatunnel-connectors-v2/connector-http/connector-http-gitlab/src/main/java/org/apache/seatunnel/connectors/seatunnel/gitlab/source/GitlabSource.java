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

package org.apache.seatunnel.connectors.seatunnel.gitlab.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.gitlab.source.config.GitlabSourceParameter;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSource;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GitlabSource extends HttpSource {
    private final GitlabSourceParameter gitlabSourceParameter = new GitlabSourceParameter();

    public GitlabSource(ReadonlyConfig pluginConfig) {
        super(pluginConfig);
        this.gitlabSourceParameter.buildWithConfig(pluginConfig);
    }

    @Override
    public String getPluginName() {
        return "Gitlab";
    }

    @Override
    public Boundedness getBoundedness() {
        if (JobMode.BATCH.equals(jobContext.getJobMode())) {
            return Boundedness.BOUNDED;
        }
        throw new UnsupportedOperationException(
                "Gitlab source connector not support unbounded operation");
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new HttpSourceReader(
                this.gitlabSourceParameter,
                readerContext,
                this.deserializationSchema,
                jsonField,
                contentField);
    }
}
