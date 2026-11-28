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

package org.apache.seatunnel.connectors.seatunnel.udp;

import org.apache.seatunnel.api.source.SourceSplit;

/**
 * Carries one exclusive listener identity and its terminal flag through checkpoint assignment.
 *
 * <p>The reader-port count controls completion accounting; a terminal split must not bind on
 * restore.
 */
final class UdpSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    final int port;
    final boolean finished;
    final int readerPortCount;

    UdpSplit(int port, boolean finished, int readerPortCount) {
        this.port = port;
        this.finished = finished;
        this.readerPortCount = readerPortCount;
    }

    @Override
    public String splitId() {
        return Integer.toString(port);
    }
}
