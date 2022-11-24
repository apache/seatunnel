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

package org.apache.seatunnel.connectors.seatunnel.fake.config;

import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.ARRAY_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.BYTES_LENGTH;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.MAP_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.ROW_NUM;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.SPLIT_NUM;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.SPLIT_READ_INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.STRING_LENGTH;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

@Builder
@Getter
public class FakeConfig implements Serializable {
    @Builder.Default
    private int rowNum = ROW_NUM.defaultValue();
    @Builder.Default
    private int splitNum = SPLIT_NUM.defaultValue();
    @Builder.Default
    private int splitReadInterval = SPLIT_READ_INTERVAL.defaultValue();
    @Builder.Default
    private int mapSize = MAP_SIZE.defaultValue();
    @Builder.Default
    private int arraySize = ARRAY_SIZE.defaultValue();
    @Builder.Default
    private int bytesLength = BYTES_LENGTH.defaultValue();
    @Builder.Default
    private int stringLength = STRING_LENGTH.defaultValue();

    public static FakeConfig buildWithConfig(Config config) {
        FakeConfigBuilder builder = FakeConfig.builder();
        if (config.hasPath(ROW_NUM.key())) {
            builder.rowNum(config.getInt(ROW_NUM.key()));
        }
        if (config.hasPath(SPLIT_NUM.key())) {
            builder.splitNum(config.getInt(SPLIT_NUM.key()));
        }
        if (config.hasPath(SPLIT_READ_INTERVAL.key())) {
            builder.splitReadInterval(config.getInt(SPLIT_READ_INTERVAL.key()));
        }
        if (config.hasPath(MAP_SIZE.key())) {
            builder.mapSize(config.getInt(MAP_SIZE.key()));
        }
        if (config.hasPath(ARRAY_SIZE.key())) {
            builder.arraySize(config.getInt(ARRAY_SIZE.key()));
        }
        if (config.hasPath(BYTES_LENGTH.key())) {
            builder.bytesLength(config.getInt(BYTES_LENGTH.key()));
        }
        if (config.hasPath(STRING_LENGTH.key())) {
            builder.stringLength(config.getInt(STRING_LENGTH.key()));
        }
        return builder.build();
    }
}
