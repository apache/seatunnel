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

package org.apache.seatunnel.flink.source;

import com.alibaba.fastjson.JSONObject;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSource;
import org.apache.seatunnel.flink.util.SchemaUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.avro.Schema;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;

import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetRowInputFormat;
import org.apache.flink.orc.OrcRowInputFormat;
import org.apache.flink.types.Row;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;

import java.util.List;
import java.util.Map;

public class FileSource implements FlinkBatchSource<Row> {

    private static final int DEFAULT_BATCH_SIZE = 1000;

    private Config config;

    private InputFormat inputFormat;

    private static final String PATH = "path";
    private static final String SOURCE_FORMAT = "format.type";
    private static final String SCHEMA = "schema";

    @Override
    public DataSet<Row> getData(FlinkEnvironment env) {
        return env.getBatchEnvironment().createInput(inputFormat);
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return CheckConfigUtil.checkAllExists(config, PATH, SOURCE_FORMAT, SCHEMA);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        String path = config.getString(PATH);
        String format = config.getString(SOURCE_FORMAT);
        String schemaContent = config.getString(SCHEMA);
        Path filePath = new Path(path);
        switch (format) {
            case "json":
                Object jsonSchemaInfo = JSONObject.parse(schemaContent);
                RowTypeInfo jsonInfo = SchemaUtil.getTypeInformation((JSONObject) jsonSchemaInfo);
                JsonRowInputFormat jsonInputFormat = new JsonRowInputFormat(filePath, null, jsonInfo);
                inputFormat = jsonInputFormat;
                break;
            case "parquet":
                final Schema parse = new Schema.Parser().parse(schemaContent);
                final MessageType messageType = new AvroSchemaConverter().convert(parse);
                inputFormat = new ParquetRowInputFormat(filePath, messageType);
                break;
            case "orc":
                OrcRowInputFormat orcRowInputFormat = new OrcRowInputFormat(path, schemaContent, null, DEFAULT_BATCH_SIZE);
                this.inputFormat = orcRowInputFormat;
                break;
            case "csv":
                Object csvSchemaInfo = JSONObject.parse(schemaContent);
                TypeInformation[] csvType = SchemaUtil.getCsvType((List<Map<String, String>>) csvSchemaInfo);
                RowCsvInputFormat rowCsvInputFormat = new RowCsvInputFormat(filePath, csvType, true);
                this.inputFormat = rowCsvInputFormat;
                break;
            case "text":
                TextRowInputFormat textInputFormat = new TextRowInputFormat(filePath);
                inputFormat = textInputFormat;
                break;
            default:
                break;
        }

    }
}
