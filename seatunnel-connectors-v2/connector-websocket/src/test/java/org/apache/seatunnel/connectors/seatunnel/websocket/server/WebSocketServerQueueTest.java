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

package org.apache.seatunnel.connectors.seatunnel.websocket.server;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WebSocketServerQueueTest {
    @Test
    void checksCountAndByteLimitsAtomicallyIncludingEmptyMessages() {
        WebSocketServer.MessageQueue queue = new WebSocketServer.MessageQueue(2, 3);
        WebSocketServer.Message first = new WebSocketServer.Message(false, new byte[2]);
        WebSocketServer.Message empty = new WebSocketServer.Message(true, new byte[0]);
        WebSocketServer.Message full = new WebSocketServer.Message(false, new byte[3]);

        assertTrue(queue.offer(first));
        assertFalse(queue.offer(full));
        assertTrue(queue.offer(empty));
        assertFalse(queue.offer(empty));
        assertSame(first, queue.poll());
        assertTrue(queue.offer(full));
        assertSame(empty, queue.poll());
        assertSame(full, queue.poll());
        assertNull(queue.poll());

        assertTrue(queue.offer(first));
        queue.close();
        assertFalse(queue.offer(empty));
        assertNull(queue.poll());
    }
}
