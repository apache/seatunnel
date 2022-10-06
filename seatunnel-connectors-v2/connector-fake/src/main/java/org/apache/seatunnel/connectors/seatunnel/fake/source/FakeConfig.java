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

package org.apache.seatunnel.connectors.seatunnel.fake.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

@Builder
@Getter
public class FakeConfig implements Serializable {
    private static final String ROW_NUM = "row.num";
    private static final String MAP_SIZE = "map.size";
    private static final String ARRAY_SIZE = "array.size";
    private static final String BYTES_LENGTH = "bytes.length";
    private static final String STRING_LENGTH = "string.length";
    private static final int DEFAULT_ROW_NUM = 5;
    private static final int DEFAULT_MAP_SIZE = 5;
    private static final int DEFAULT_ARRAY_SIZE = 5;
    private static final int DEFAULT_BYTES_LENGTH = 5;
    private static final int DEFAULT_STRING_LENGTH = 5;
    @Builder.Default
    private int rowNum = DEFAULT_ROW_NUM;
    @Builder.Default
    private int mapSize = DEFAULT_MAP_SIZE;
    @Builder.Default
    private int arraySize = DEFAULT_ARRAY_SIZE;
    @Builder.Default
    private int bytesLength = DEFAULT_BYTES_LENGTH;
    @Builder.Default
    private int stringLength = DEFAULT_STRING_LENGTH;

    public static FakeConfig buildWithConfig(Config config) {
        FakeConfigBuilder builder = FakeConfig.builder();
        if (config.hasPath(ROW_NUM)) {
            builder.rowNum(config.getInt(ROW_NUM));
        }
        if (config.hasPath(MAP_SIZE)) {
            builder.mapSize(config.getInt(MAP_SIZE));
        }
        if (config.hasPath(ARRAY_SIZE)) {
            builder.arraySize(config.getInt(ARRAY_SIZE));
        }
        if (config.hasPath(BYTES_LENGTH)) {
            builder.bytesLength(config.getInt(BYTES_LENGTH));
        }
        if (config.hasPath(STRING_LENGTH)) {
            builder.stringLength(config.getInt(STRING_LENGTH));
        }
        return builder.build();
    }
}
