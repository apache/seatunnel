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

package org.apache.seatunnel.edge.agent.transport.socket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public interface EdgeSocketSocketFactory {

    /**
     * Opens a connected TCP socket to the EdgeSocket ingress.
     *
     * <p>Called during session establishment and reconnect. Implementations should enable {@code
     * TCP_NODELAY} when appropriate and close the socket if connect fails after partial setup.
     *
     * @param address resolved {@code host:port} from output configuration
     * @param connectTimeoutMs connect timeout in milliseconds
     * @return connected socket (caller owns lifecycle)
     * @throws IOException if connect fails or times out
     */
    Socket connect(InetSocketAddress address, int connectTimeoutMs) throws IOException;

    EdgeSocketSocketFactory DEFAULT =
            new EdgeSocketSocketFactory() {
                @Override
                public Socket connect(InetSocketAddress address, int connectTimeoutMs)
                        throws IOException {
                    Socket socket = new Socket();
                    try {
                        socket.connect(address, connectTimeoutMs);
                        socket.setTcpNoDelay(true);
                        return socket;
                    } catch (IOException ex) {
                        try {
                            socket.close();
                        } catch (IOException ignored) {
                            // ignore close failures
                        }
                        throw ex;
                    }
                }
            };
}
