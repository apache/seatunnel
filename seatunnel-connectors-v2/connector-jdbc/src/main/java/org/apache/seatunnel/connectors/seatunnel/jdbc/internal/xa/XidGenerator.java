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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.sink.SinkWriter;

import javax.transaction.xa.Xid;

import java.io.Serializable;
import java.security.SecureRandom;

/**
 * {@link Xid} generator.
 */
public interface XidGenerator
    extends Serializable, AutoCloseable {

    Xid generateXid(JobContext context, SinkWriter.Context sinkContext, long checkpointId);

    default void open() {}

    /**
     * @return true if the provided transaction belongs to this subtask
     */
    boolean belongsToSubtask(Xid xid, JobContext context, SinkWriter.Context sinkContext);

    @Override
    default void close() {}

    /**
     * Creates a {@link XidGenerator} that generates {@link Xid xids} from:
     *
     * <ol>
     *   <li>job id
     *   <li>subtask index
     *   <li>checkpoint id
     *   <li>four random bytes generated using {@link SecureRandom})
     * </ol>
     *
     * <p>Each created {@link XidGenerator} instance MUST be used for only one Sink instance
     * (otherwise Xids could collide).
     */
    static XidGenerator semanticXidGenerator() {
        return new SemanticXidGenerator();
    }
}
