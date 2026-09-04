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

package org.apache.seatunnel.connectors.seatunnel.azure.queue.source;

import lombok.Getter;

import java.util.Arrays;

/** A received queue message and the mutable pop receipt needed to manage its lease. */
@Getter
final class AzureQueueMessage {

    private final String messageId;
    private final String messageText;
    private final byte[] body;
    private String popReceipt;
    private boolean deleted;

    AzureQueueMessage(String messageId, String popReceipt, String messageText, byte[] body) {
        this.messageId = messageId;
        this.popReceipt = popReceipt;
        this.messageText = messageText;
        this.body = Arrays.copyOf(body, body.length);
    }

    void updatePopReceipt(String popReceipt) {
        this.popReceipt = popReceipt;
    }

    void markDeleted() {
        this.deleted = true;
    }
}
