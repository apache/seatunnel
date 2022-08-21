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

package org.apache.seatunnel.connectors.seatunnel.hive.source.file.reader.format;

import org.apache.seatunnel.connectors.seatunnel.hive.source.file.reader.type.FileTypeEnum;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReadStrategyFactory {

    private ReadStrategyFactory() {}

    public static ReadStrategy of(String fileType) {
        try {
            FileTypeEnum fileTypeEnum = FileTypeEnum.valueOf(fileType.toUpperCase());
            return fileTypeEnum.getReadStrategy();
        } catch (IllegalArgumentException e) {
            log.warn("Hive plugin not support this file type [{}], it will be treated as a plain text file", fileType);
            return new TextReadStrategy();
        }
    }
}
