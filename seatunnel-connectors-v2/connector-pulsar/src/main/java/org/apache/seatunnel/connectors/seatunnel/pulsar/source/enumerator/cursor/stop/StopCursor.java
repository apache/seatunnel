/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.stop;

import org.apache.seatunnel.api.source.Boundedness;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;

import java.io.Serializable;

public interface StopCursor extends Serializable {

    /**
     * Determine whether to pause consumption on the current message by the returned boolean value.
     * The message presented in method argument wouldn't be consumed if the return result is true.
     */
    boolean shouldStop(Message<?> message);

    default Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    StopCursor copy();

    // --------------------------- Static Factory Methods -----------------------------

    static StopCursor never() {
        return NeverStopCursor.INSTANCE;
    }

    static StopCursor latest() {
        return new LatestMessageStopCursor();
    }

    static StopCursor atMessageId(MessageId messageId) {
        return new MessageIdStopCursor(messageId);
    }

    static StopCursor afterMessageId(MessageId messageId) {
        return new MessageIdStopCursor(messageId, false);
    }

    static StopCursor timestamp(long timestamp) {
        return new TimestampStopCursor(timestamp);
    }
}
