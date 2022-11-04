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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplit;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplitEnumeratorState;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

@AutoService(SeaTunnelSource.class)
public class RabbitmqSource implements SeaTunnelSource<SeaTunnelRow, RabbitmqSplit, RabbitmqSplitEnumeratorState> {

    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private JobContext jobContext;
    private RabbitmqConfig rabbitMQConfig;

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
        this.rabbitMQConfig = new RabbitmqConfig(config);
        setDeserialization(config);
    }

    @Override
    public SeaTunnelDataType getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @Override
    public SourceReader<SeaTunnelRow, RabbitmqSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new RabbitmqSourceReader(deserializationSchema, readerContext, rabbitMQConfig);
    }

    @Override
    public SourceSplitEnumerator<RabbitmqSplit, RabbitmqSplitEnumeratorState> createEnumerator(SourceSplitEnumerator.Context<RabbitmqSplit> enumeratorContext) throws Exception {
        return new RabbitmqSplitEnumerator();
    }

    @Override
    public SourceSplitEnumerator<RabbitmqSplit, RabbitmqSplitEnumeratorState> restoreEnumerator(SourceSplitEnumerator.Context<RabbitmqSplit> enumeratorContext, RabbitmqSplitEnumeratorState checkpointState) throws Exception {
        return new RabbitmqSplitEnumerator();
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    private void setDeserialization(Config config) {
        // TODO: format SPI
        //only support json deserializationSchema
        SeaTunnelRowType rowType = SeaTunnelSchema.buildWithConfig(config.getConfig(SeaTunnelSchema.SCHEMA)).getSeaTunnelRowType();
        this.deserializationSchema = new JsonDeserializationSchema(false, false, rowType);
    }
}
