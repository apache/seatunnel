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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;

public class BlockingIngressChannel implements IngressChannel {

    private final Socket socket;
    private final BufferedReader reader;
    private final BufferedWriter writer;

    public BlockingIngressChannel(Socket socket) throws IOException {
        this.socket = socket;
        this.reader =
                new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        this.writer =
                new BufferedWriter(
                        new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
    }

    @Override
    public String readLine() throws IOException {
        return reader.readLine();
    }

    @Override
    public void writeLine(String response) throws IOException {
        writer.write(response);
        writer.newLine();
        writer.flush();
    }

    @Override
    public SocketAddress remoteAddress() {
        return socket.getRemoteSocketAddress();
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
