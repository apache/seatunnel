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

package org.apache.seatunnel.flink.kafka.sink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.Preconditions;

public enum SinkFormatEnum {
    JSON,
    CSV,
    ;

    public static SinkFormatEnum fromName(Object name) {
        Preconditions.checkState(StringUtils.isNotBlank(String.valueOf(name)), "Format name is null or blank.");
        for (SinkFormatEnum e : SinkFormatEnum.values()) {
            if (e.name().equalsIgnoreCase(String.valueOf(name))) {
                return e;
            }
        }
        throw new RuntimeException("Unknown format type, the type of format should be 'Json' or 'Csv' Only.");
    }
}
