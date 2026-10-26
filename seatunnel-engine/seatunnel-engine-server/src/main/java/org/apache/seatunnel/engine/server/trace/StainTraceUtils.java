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

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.Map;

/** Helper methods for reading, mutating, and extending stain trace payloads on row options. */
public final class StainTraceUtils {
    private StainTraceUtils() {}

    public static byte[] getPayloadOrNull(SeaTunnelRow row) {
        if (row == null) {
            return null;
        }
        Map<String, Object> options = row.getOptionsOrNull();
        if (options == null) {
            return null;
        }
        Object payload = options.get(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY);
        return payload instanceof byte[] ? (byte[]) payload : null;
    }

    public static boolean hasPayload(SeaTunnelRow row) {
        return getPayloadOrNull(row) != null;
    }

    public static void setPayload(SeaTunnelRow row, byte[] payload) {
        ensureMutableOptions(row).put(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY, payload);
    }

    public static void removePayload(SeaTunnelRow row) {
        Map<String, Object> options = row.getOptionsOrNull();
        if (options == null) {
            return;
        }
        if (!options.containsKey(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY)) {
            return;
        }
        ensureMutableOptions(row).remove(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY);
    }

    /** Appends one stage to the payload only when the row already carries a valid stain trace. */
    public static void appendIfPresent(
            SeaTunnelRow row,
            StainTraceStage stage,
            long taskId,
            long tsMs,
            int maxEntries,
            Counter entriesTruncatedTotal) {
        Map<String, Object> options = row.getOptionsOrNull();
        if (options == null) {
            return;
        }
        Object payloadObj = options.get(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY);
        if (!(payloadObj instanceof byte[])) {
            return;
        }
        byte[] payload = (byte[]) payloadObj;
        StainTracePayload.AppendResult result =
                StainTracePayload.append(payload, stage, taskId, tsMs, maxEntries);
        if (result.getStatus() == StainTracePayload.AppendStatus.TRUNCATED) {
            if (entriesTruncatedTotal != null) {
                entriesTruncatedTotal.inc();
            }
            return;
        }
        if (result.getStatus() == StainTracePayload.AppendStatus.INVALID) {
            return;
        }
        if (result.getPayload() != payload) {
            ensureMutableOptions(row)
                    .put(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY, result.getPayload());
        }
    }

    private static Map<String, Object> ensureMutableOptions(SeaTunnelRow row) {
        return row.getOptions();
    }
}
