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

package org.apache.seatunnel.connectors.seatunnel.kudu.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient.KuduOutputFormat;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class KuduSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private SeaTunnelRowType seaTunnelRowType;
    private Config pluginConfig;
    private KuduOutputFormat fileWriter;
    private KuduSinkConfig kuduSinkConfig;

    public KuduSinkWriter(@NonNull SeaTunnelRowType seaTunnelRowType,
                          @NonNull Config pluginConfig) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.pluginConfig = pluginConfig;

        kuduSinkConfig = new KuduSinkConfig(this.pluginConfig);
        fileWriter = new KuduOutputFormat(kuduSinkConfig);

    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        fileWriter.write(element);
    }

    @Override
    public void close() throws IOException {
        fileWriter.closeOutputFormat();
    }
}
