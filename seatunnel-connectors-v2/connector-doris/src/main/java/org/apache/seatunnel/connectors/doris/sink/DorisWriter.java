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

package org.apache.seatunnel.connectors.doris.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.doris.common.DorisConstants;
import org.apache.seatunnel.connectors.doris.common.DorisOptions;
import org.apache.seatunnel.connectors.doris.sink.loader.DorisStreamLoader;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created 2022/8/01
 */
public class DorisWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(DorisWriter.class);

    private static final String LABEL_TEMPLATE = "setunnel_sink_subtask_%s_%s_%s";

    private final SeaTunnelRowType seaTunnelRowType;
    private final DorisOptions options;
    private final JsonSerializationSchema serializationSchema;
    private final DorisLoader<String> loader;

    private final List<JsonNode> batch;
    private final DateTimeFormatter formatter;
    private final SinkWriter.Context context;

    public DorisWriter(Config dorisSinkConf,
                       SeaTunnelRowType seaTunnelRowType,
                       SinkWriter.Context context) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.options = DorisOptions.fromPluginConfig(dorisSinkConf);
        this.serializationSchema = new JsonSerializationSchema(seaTunnelRowType);
        //now we only support stream load, maybe future broker load will implement in seatunnel.
        this.loader = new DorisStreamLoader(options);
        this.batch = new ArrayList<>();
        this.formatter = DateTimeFormatter.ofPattern(DorisConstants.DORIS_LABEL_PATTERN_VALUE)
            .withZone(ZoneId.systemDefault());
        this.context = context;
        LOG.info("Subtask {} create doris writer.", context.getIndexOfSubtask());
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        batch.add(JsonUtils.stringToJsonNode(new String(serializationSchema.serialize(element))));
        if (options.getBatchSize() > 0 && batch.size() >= options.getBatchSize()) {
            flush();
        }
    }

    private synchronized void flush() {
        if (CollectionUtils.isEmpty(batch)) {
            return;
        }
        String request = JsonUtils.toJsonString(batch);
        String label = String.format(LABEL_TEMPLATE, context.getIndexOfSubtask(),
            formatter.format(Instant.now()), UUID.randomUUID());
        loader.load(request, label);
        batch.clear();
    }

    @Override
    public void close() throws IOException {
        flush();
        loader.close();
    }
}
