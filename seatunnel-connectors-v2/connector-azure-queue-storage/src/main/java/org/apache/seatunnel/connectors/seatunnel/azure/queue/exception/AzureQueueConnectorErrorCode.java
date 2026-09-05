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

package org.apache.seatunnel.connectors.seatunnel.azure.queue.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum AzureQueueConnectorErrorCode implements SeaTunnelErrorCode {
    CONNECTION_FAILED("AzureQueueStorage-01", "Create Azure Queue Storage client failed"),
    WRITE_FAILED("AzureQueueStorage-02", "Send Azure Queue Storage message failed"),
    MESSAGE_TOO_LARGE("AzureQueueStorage-03", "Azure Queue Storage message is too large"),
    CLOSE_FAILED("AzureQueueStorage-04", "Close Azure Queue Storage sender failed"),
    READ_FAILED("AzureQueueStorage-05", "Read Azure Queue Storage message failed"),
    ACKNOWLEDGE_FAILED("AzureQueueStorage-06", "Delete Azure Queue Storage message failed"),
    VISIBILITY_RENEWAL_FAILED(
            "AzureQueueStorage-07", "Renew Azure Queue Storage message visibility failed"),
    CONFIGURATION_FAILED("AzureQueueStorage-08", "Azure Queue Storage configuration is invalid");

    private final String code;
    private final String description;

    AzureQueueConnectorErrorCode(String code, String description) {
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
