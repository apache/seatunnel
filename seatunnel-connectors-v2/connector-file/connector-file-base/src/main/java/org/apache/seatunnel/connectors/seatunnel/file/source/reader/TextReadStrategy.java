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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FilePluginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class TextReadStrategy extends AbstractReadStrategy {

    private static final String TEXT_FIELD_NAME = "lines";

    @Override
    public void read(String path, Collector<SeaTunnelRow> output) throws IOException, FilePluginException {
        Configuration conf = getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path filePath = new Path(path);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath), StandardCharsets.UTF_8))) {
            reader.lines().forEach(line -> output.collect(new SeaTunnelRow(new String[]{line})));
        }
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(HadoopConf hadoopConf, String path) {
        return new SeaTunnelRowType(new String[]{TEXT_FIELD_NAME},
                new SeaTunnelDataType<?>[]{BasicType.STRING_TYPE});
    }
}
