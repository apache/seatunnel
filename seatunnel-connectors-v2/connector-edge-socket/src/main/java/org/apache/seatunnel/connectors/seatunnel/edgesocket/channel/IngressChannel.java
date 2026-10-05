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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.channel;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;

public interface IngressChannel extends Closeable {

    /**
     * Read one line from the collector.
     *
     * @return the line content, or {@code null} if the connection is closed
     * @throws java.net.SocketTimeoutException if the read timed out
     * @throws IOException on I/O error
     */
    String readLine() throws IOException;

    /**
     * Write a response line to the collector (includes newline and flush).
     *
     * @param response the response string to send
     * @throws IOException on I/O error
     */
    void writeLine(String response) throws IOException;

    /** Remote address for logging purposes. */
    SocketAddress remoteAddress();
}
