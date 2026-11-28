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

package org.apache.seatunnel.e2e.connector.udp;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.Base64;

/**
 * Sends exact datagram bytes from an independent JVM in the test engine network namespace.
 *
 * <p>Exclusive local binding also probes actual listener release rather than inferring it from
 * logs.
 */
public final class UdpDatagramSender {
    private UdpDatagramSender() {}

    public static void main(String[] args) throws Exception {
        String operation = args[0];
        InetSocketAddress local = new InetSocketAddress("127.0.0.1", Integer.parseInt(args[1]));
        try (DatagramSocket socket = new DatagramSocket(null)) {
            socket.setReuseAddress(false);
            socket.bind(local);
            if ("probe".equals(operation)) {
                return;
            }
            InetSocketAddress destination =
                    new InetSocketAddress("127.0.0.1", Integer.parseInt(args[2]));
            for (int i = 3; i < args.length; i++) {
                byte[] payload = Base64.getDecoder().decode(args[i]);
                socket.send(new DatagramPacket(payload, payload.length, destination));
            }
        }
    }
}
