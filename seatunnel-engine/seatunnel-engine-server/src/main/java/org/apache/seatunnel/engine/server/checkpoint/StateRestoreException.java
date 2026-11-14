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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import java.util.Set;
import java.util.TreeSet;

/**
 * Thrown when connector-owned checkpoint state cannot be read back during a restore, for example
 * after a connector upgrade changed one of its state classes. The message names the action, job and
 * pipeline, and the original exception is kept as the cause.
 */
public class StateRestoreException extends SeaTunnelEngineException {

    private static final long serialVersionUID = 1L;

    public StateRestoreException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Deserializes one piece of connector-owned state (enumerator state, a split, writer state or
     * aggregated commit info) while an action restores from a checkpoint.
     *
     * @param serializer the connector's serializer for this kind of state
     * @param bytes the state bytes stored in the checkpoint
     * @param actionName the name of the action that owns the state
     * @param pluginName the connector plugin name, so the user knows which connector to look up
     * @param location the task that restores the state
     * @return the restored state
     * @throws StateRestoreException if the serializer cannot read the bytes
     */
    public static <T> T deserialize(
            Serializer<T> serializer,
            byte[] bytes,
            String actionName,
            String pluginName,
            TaskLocation location) {
        try {
            return serializer.deserialize(bytes);
        } catch (Exception e) {
            throw new StateRestoreException(
                    String.format(
                            "Cannot restore state of action '%s' (job %d, pipeline %d): the state"
                                    + " could not be deserialized, possibly because it was"
                                    + " produced by an incompatible connector version (version"
                                    + " unknown). If you upgraded, check the Incompatible Changes"
                                    + " document for %s. Cause: %s",
                            actionName,
                            location.getJobId(),
                            location.getPipelineId(),
                            pluginName,
                            e),
                    e);
        }
    }

    /**
     * Returns the connector name of a sink for the restore error message. The engine wraps sinks in
     * a {@link MultiTableSink}, whose own plugin name does not tell the user which connector to
     * look up, so the names of the wrapped sinks are used instead.
     *
     * @param sink the sink of the action that restores its state
     * @return the plugin names of the wrapped sinks, or the sink's own plugin name
     */
    public static String sinkPluginName(SeaTunnelSink<?, ?, ?, ?> sink) {
        if (sink instanceof MultiTableSink) {
            Set<String> names = new TreeSet<>();
            for (SeaTunnelSink<?, ?, ?, ?> wrapped : ((MultiTableSink) sink).getSinks().values()) {
                names.add(wrapped.getPluginName());
            }
            if (!names.isEmpty()) {
                return String.join(", ", names);
            }
        }
        return sink.getPluginName();
    }
}
