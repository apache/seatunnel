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

package org.apache.seatunnel.engine.server.trace;

public enum StainTraceStage {
    SOURCE_EMIT((byte) 1),
    QUEUE_IN((byte) 2),
    QUEUE_OUT((byte) 3),
    TRANSFORM_IN((byte) 4),
    TRANSFORM_OUT((byte) 5),
    SINK_WRITE_DONE((byte) 6);

    private final byte code;

    StainTraceStage(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }
}
