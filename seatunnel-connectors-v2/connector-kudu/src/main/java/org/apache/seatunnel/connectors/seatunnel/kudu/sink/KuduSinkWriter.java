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

import lombok.NonNull;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient.KuduOutputFormat;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class KuduSinkWriter implements SinkWriter<SeaTunnelRow, KuduCommitInfo, KuduSinkState> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KuduSinkWriter.class);

    private SeaTunnelRowType seaTunnelRowType;
    private Config pluginConfig;
    private Context context;
    private long jobId;

    private KuduOutputFormat fileWriter;

    private KuduSinkConfig kuduSinkConfig;

    public KuduSinkWriter(@NonNull SeaTunnelRowType seaTunnelRowType,
                          @NonNull Config pluginConfig,
                          @NonNull Context context,
                          long jobId) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.pluginConfig = pluginConfig;
        this.context = context;
        this.jobId = jobId;

        kuduSinkConfig = new KuduSinkConfig(this.pluginConfig);
        fileWriter = new KuduOutputFormat(kuduSinkConfig);
      //  fileWriter.init();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        fileWriter.write(element);
    }

    @Override
    public Optional<KuduCommitInfo> prepareCommit() throws IOException {
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {

    }


    @Override
    public void close() throws IOException {

    }


}
