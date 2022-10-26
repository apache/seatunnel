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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.source;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitMQSplit;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitMQState;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

@AutoService(SeaTunnelSource.class)
public class RabbitMQSource implements SeaTunnelSource<SeaTunnelRow, RabbitMQSplit, RabbitMQState> {

    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private SeaTunnelRowType typeInfo;
    private JobContext jobContext;

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode()) ? Boundedness.BOUNDED : Boundedness.UNBOUNDED;
    }

    @Override
    public String getPluginName() {
        return "RabbitMQ";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {

    }

    @Override
    public SeaTunnelRowType getProducedType() {
        return this.typeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, RabbitMQSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return null;
    }

    @Override
    public SourceSplitEnumerator<RabbitMQSplit, RabbitMQState> createEnumerator(SourceSplitEnumerator.Context<RabbitMQSplit> enumeratorContext) throws Exception {
        return null;
    }

    @Override
    public SourceSplitEnumerator<RabbitMQSplit, RabbitMQState> restoreEnumerator(SourceSplitEnumerator.Context<RabbitMQSplit> enumeratorContext, RabbitMQState checkpointState) throws Exception {
        return null;
    }


    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    private void setDeserialization(Config config) {

    }
}
