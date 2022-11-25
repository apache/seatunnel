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

package org.apache.seatunnel.connectors.seatunnel.kudu.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum KuduConnectorErrorCode implements SeaTunnelErrorCode {
    GET_KUDUSCAN_OBJECT_FAILED("KUDU-01", "Get the Kuduscan object for each splice failed"),
    CLOSE_KUDU_CLIENT_FAILED("KUDU-02", "Close Kudu client failed"),
    DATA_TYPE_CAST_FILED("KUDU-03", "Value type does not match column type"),
    KUDU_UPSERT_FAILED("KUDU-04", "Upsert data to Kudu failed"),
    KUDU_INSERT_FAILED("KUDU-05", "Insert data to Kudu failed"),
    INIT_KUDU_CLIENT_FAILED("KUDU-06", "Initialize the Kudu client failed"),
    GENERATE_KUDU_PARAMETERS_FAILED("KUDU-07", "Generate Kudu Parameters in the preparation phase failed")
    ;



    private final String code;

    private final String description;

    KuduConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }
}
