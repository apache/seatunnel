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

package org.apache.seatunnel.connectors.seatunnel.google.pubsub.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum GooglePubSubConnectorErrorCode implements SeaTunnelErrorCode {
    CONNECTION_FAILED("GooglePubSub-01", "Create Google Pub/Sub client failed"),
    WRITE_FAILED("GooglePubSub-02", "Publish Google Pub/Sub message failed"),
    CLOSE_FAILED("GooglePubSub-03", "Close Google Pub/Sub client failed"),
    READ_FAILED("GooglePubSub-04", "Read Google Pub/Sub message failed"),
    ACKNOWLEDGE_FAILED("GooglePubSub-05", "Acknowledge Google Pub/Sub message failed"),
    CONFIGURATION_FAILED("GooglePubSub-06", "Validate Google Pub/Sub configuration failed");

    private final String code;
    private final String description;

    GooglePubSubConnectorErrorCode(String code, String description) {
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
