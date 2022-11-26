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

package org.apache.seatunnel.connectors.seatunnel.socket.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;

@Slf4j
public class SocketSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private static final int CHAR_BUFFER_SIZE = 8192;
    private final SocketSourceParameter parameter;
    private final SingleSplitReaderContext context;
    private Socket socket;
    private final String delimiter = "\n";

    SocketSourceReader(SocketSourceParameter parameter, SingleSplitReaderContext context) {
        this.parameter = parameter;
        this.context = context;
    }

    @Override
    public void open() throws Exception {
        socket = new Socket();
        log.info("connect socket server, host:[{}], port:[{}] ", this.parameter.getHost(), this.parameter.getPort());
        socket.connect(new InetSocketAddress(this.parameter.getHost(), this.parameter.getPort()), 0);
    }

    @Override
    public void close() throws IOException {
        if (socket != null) {
            socket.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        StringBuilder buffer = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            char[] buf = new char[CHAR_BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = reader.read(buf)) != -1) {
                buffer.append(buf, 0, bytesRead);

                int delimPos;
                while (buffer.length() >= this.delimiter.length() && (delimPos = buffer.indexOf(this.delimiter)) != -1) {
                    String record = buffer.substring(0, delimPos);
                    if (record.endsWith("\r")) {
                        record = record.substring(0, record.length() - 1);
                    }
                    output.collect(new SeaTunnelRow(new Object[] {record}));
                    buffer.delete(0, delimPos + this.delimiter.length());
                }
                if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
                    // signal to the source that we have reached the end of the data.
                    context.signalNoMoreElement();
                    break;
                }
            }
        }
        if (buffer.length() > 0) {
            output.collect(new SeaTunnelRow(new Object[] {buffer.toString()}));
        }
    }
}
