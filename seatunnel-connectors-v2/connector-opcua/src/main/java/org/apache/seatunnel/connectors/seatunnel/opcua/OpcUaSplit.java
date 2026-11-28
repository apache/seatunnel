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

package org.apache.seatunnel.connectors.seatunnel.opcua;

import org.apache.seatunnel.api.source.SourceSplit;

import java.util.Collections;
import java.util.Map;

/** Connector-owned state. A live source checkpoint has no remote notification offset. */
final class OpcUaSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;
    final String stateKey;
    final Map<String, String> bindings;
    final long emitted;
    final boolean finished;

    OpcUaSplit(String stateKey) {
        this(stateKey, Collections.emptyMap(), 0L, false);
    }

    OpcUaSplit(String stateKey, Map<String, String> bindings, long emitted, boolean finished) {
        this.stateKey = stateKey;
        this.bindings = Map.copyOf(bindings);
        this.emitted = emitted;
        this.finished = finished;
    }

    @Override
    public String splitId() {
        return "opcua-acquisition";
    }
}
