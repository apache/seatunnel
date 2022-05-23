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

package org.apache.seatunnel.connectors.seatunnel.kafka.source;

import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.BOOTSTRAP_SERVER;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.TOPIC;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.Properties;

public class KafkaSource implements SeaTunnelSource<SeaTunnelRow, KafkaSourceSplit, KafkaState> {


    private String topic;
    private String bootstrapServer;
    private Properties properties;

    @Override
    public String getPluginName() {
        return "Kafka";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(config, TOPIC, BOOTSTRAP_SERVER);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }
        this.topic = config.getString(TOPIC);
        this.bootstrapServer = config.getString(BOOTSTRAP_SERVER);
        // TODO add user custom properties
        this.properties = new Properties();
    }

    @Override
    public SeaTunnelRowTypeInfo getRowTypeInfo() {
        return new SeaTunnelRowTypeInfo(new String[]{"topic", "raw_message"},
                new SeaTunnelDataType[]{BasicType.STRING, BasicType.STRING});
    }

    @Override
    public SourceReader<SeaTunnelRow, KafkaSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new KafkaSourceReader(this.topic, this.bootstrapServer);
    }

    @Override
    public SourceSplitEnumerator<KafkaSourceSplit, KafkaState> createEnumerator(SourceSplitEnumerator.Context<KafkaSourceSplit> enumeratorContext) throws Exception {
        return new KafkaSourceSplitEnumerator(this.topic, this.bootstrapServer, this.properties);
    }

    @Override
    public SourceSplitEnumerator<KafkaSourceSplit, KafkaState> restoreEnumerator(SourceSplitEnumerator.Context<KafkaSourceSplit> enumeratorContext, KafkaState checkpointState) throws Exception {
        // TODO support state restore
        return new KafkaSourceSplitEnumerator(this.topic, this.bootstrapServer, this.properties);
    }

    @Override
    public Serializer<KafkaState> getEnumeratorStateSerializer() {
        return new DefaultSerializer<>();
    }
}
