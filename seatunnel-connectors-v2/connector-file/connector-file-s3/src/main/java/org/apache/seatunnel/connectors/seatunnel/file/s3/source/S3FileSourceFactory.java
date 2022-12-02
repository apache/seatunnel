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

package org.apache.seatunnel.connectors.seatunnel.file.s3.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3Config;

import com.google.auto.service.AutoService;

import java.util.Arrays;

@AutoService(Factory.class)
public class S3FileSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return FileSystemType.S3.getFileSystemPluginName();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(S3Config.FILE_PATH)
                .required(S3Config.FILE_TYPE)
                .required(S3Config.S3_BUCKET)
                .required(S3Config.S3_ACCESS_KEY)
                .required(S3Config.S3_SECRET_KEY)
                .optional(S3Config.DELIMITER)
                .optional(S3Config.PARSE_PARTITION_FROM_PATH)
                .optional(S3Config.DATE_FORMAT)
                .optional(S3Config.DATETIME_FORMAT)
                .optional(S3Config.TIME_FORMAT)
                .conditional(S3Config.FILE_TYPE, Arrays.asList("text", "json"), SeaTunnelSchema.SCHEMA)
                .build();
    }
}
