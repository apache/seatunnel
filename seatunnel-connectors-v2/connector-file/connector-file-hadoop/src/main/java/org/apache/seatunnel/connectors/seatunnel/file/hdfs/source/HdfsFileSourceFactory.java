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

package org.apache.seatunnel.connectors.seatunnel.file.hdfs.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.config.HdfsSourceConfig;

import com.google.auto.service.AutoService;

import java.util.Arrays;

@AutoService(Factory.class)
public class HdfsFileSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return FileSystemType.HDFS.getFileSystemPluginName();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HdfsSourceConfig.FILE_PATH)
                .required(HdfsSourceConfig.FILE_TYPE)
                .required(HdfsSourceConfig.DEFAULT_FS)
                .optional(HdfsSourceConfig.DELIMITER)
                .optional(HdfsSourceConfig.PARSE_PARTITION_FROM_PATH)
                .optional(HdfsSourceConfig.DATE_FORMAT)
                .optional(HdfsSourceConfig.DATETIME_FORMAT)
                .optional(HdfsSourceConfig.TIME_FORMAT)
                .conditional(HdfsSourceConfig.FILE_TYPE, Arrays.asList("text", "json"), SeaTunnelSchema.SCHEMA)
                .build();
    }
}
