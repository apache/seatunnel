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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum HugeGraphConnectorErrorCode implements SeaTunnelErrorCode {
    BUILD_CLIENT_FAILED("HUGEGRAPH-01", "Build HugeGraph Client failed"),
    GRAPH_OPERATION_FAILED("HUGEGRAPH-02", "Writing graph element failed"),
    OPERATION_RETRY_INTERRUPTED("HUGEGRAPH-03", "Graph operation retried interrupted"),
    ASYNCHRONOUS_FLUSH_FAILED("HUGEGRAPH-04", "Asynchronous flush failed"),
    BUFFER_ADD_FAILED("HUGEGRAPH-05", "BatchBuffer is already closed."),
    INVALID_GRAPH_SCHEMA("HUGEGRAPH-06", "Invalid Graph Schema"),
    ILLEGAL_CONFIG_ARGUMENT("HUGEGRAPH-07", "Illegal argument"),
    ;

    private final String code;
    private final String description;

    HugeGraphConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
