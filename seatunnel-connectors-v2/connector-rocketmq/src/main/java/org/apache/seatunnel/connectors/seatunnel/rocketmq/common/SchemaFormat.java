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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.common;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;

/** schema format type */
public enum SchemaFormat {
    JSON("json"),
    TEXT("text");

    private final String name;

    SchemaFormat(String name) {
        this.name = name;
    }

    /** find format */
    public static SchemaFormat find(String name) {
        for (SchemaFormat format : values()) {
            if (format.getName().equals(name)) {
                return format;
            }
        }
        throw new SeaTunnelJsonFormatException(
                CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Unsupported format: " + name);
    }

    public String getName() {
        return name;
    }
}
