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

package org.apache.seatunnel.connectors.seatunnel.http.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import com.google.auto.service.AutoService;
import com.jayway.jsonpath.JsonPath;

@AutoService(SeaTunnelSource.class)
public class HttpSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    protected final HttpParameter httpParameter = new HttpParameter();
    protected SeaTunnelRowType rowType;
    protected JsonField jsonField;
    protected JobContext jobContext;
    protected DeserializationSchema<SeaTunnelRow> deserializationSchema;
    protected JsonPath[] jsonPaths;

    @Override
    public String getPluginName() {
        return "Http";
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode()) ? Boundedness.BOUNDED : Boundedness.UNBOUNDED;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, HttpConfig.URL.key());
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }
        this.httpParameter.buildWithConfig(pluginConfig);
        buildSchemaWithConfig(pluginConfig);
    }

    protected void buildSchemaWithConfig(Config pluginConfig) {
        if (pluginConfig.hasPath(SeaTunnelSchema.SCHEMA.key())) {
            Config schema = pluginConfig.getConfig(SeaTunnelSchema.SCHEMA.key());
            this.rowType = SeaTunnelSchema.buildWithConfig(schema).getSeaTunnelRowType();
            // default use json format
            String format = HttpConfig.DEFAULT_FORMAT;
            if (pluginConfig.hasPath(HttpConfig.FORMAT.key())) {
                format = pluginConfig.getString(HttpConfig.FORMAT.key());
            }
            switch (format) {
                case HttpConfig.DEFAULT_FORMAT:
                    this.deserializationSchema = new JsonDeserializationSchema(false, false, rowType);
                    if (pluginConfig.hasPath(HttpConfig.JSON_FIELD.key())) {
                        jsonField = getJsonField(pluginConfig.getConfig(HttpConfig.JSON_FIELD.key()));
                        this.initJsonPath(jsonField);
                    }
                    break;
                default:
                    // TODO: use format SPI
                    throw new UnsupportedOperationException("Unsupported format: " + format);
            }
        } else {
            this.rowType = SeaTunnelSchema.buildSimpleTextSchema();
            this.deserializationSchema = new SimpleTextDeserializationSchema(this.rowType);
        }
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowType;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext readerContext) throws Exception {
        return new HttpSourceReader(this.httpParameter, readerContext, this.deserializationSchema, jsonField, jsonPaths);
    }

    private JsonField getJsonField(Config jsonFieldConf) {
        ConfigRenderOptions options = ConfigRenderOptions.concise();

        return JsonField.builder().fields(JsonUtils.toMap(jsonFieldConf.root().render(options))).build();
    }

    private void initJsonPath(JsonField jsonField) {
        jsonPaths = new JsonPath[jsonField.getFields().size()];
        final int[] index = {0};
        jsonField.getFields().forEach((key, value) -> {
            jsonPaths[index[0]] = JsonPath.compile(jsonField.getFields().get(key));
            index[0]++;
        });
    }
}
