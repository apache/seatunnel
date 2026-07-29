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

package org.apache.seatunnel.edge.agent.transport.console;

import org.apache.seatunnel.edge.agent.transport.EdgeCollectorTransport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsoleCollectorTransport implements EdgeCollectorTransport {

    private static final Logger LOG = LoggerFactory.getLogger(ConsoleCollectorTransport.class);

    @Override
    public void open() {}

    @Override
    public void sendUntilReceived(long batchId, String payload) {
        LOG.info("EDGE_CONSOLE_OUTPUT batchId={} payload={}", batchId, payload);
    }

    @Override
    public boolean probeReachable() {
        return true;
    }

    @Override
    public void close() {}
}
