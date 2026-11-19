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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.source;

import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplitEnumeratorState;

import java.util.Collections;

/**
 * Legacy checkpoint state created by RabbitMQ sources before the split enumerator state fix.
 *
 * @deprecated only used to restore checkpoints written by older connector versions
 */
@Deprecated
public class RabbitmqSourceState extends RabbitmqSplitEnumeratorState {
    private static final long serialVersionUID = -1143819030309308746L;

    public RabbitmqSourceState() {
        super(Collections.emptyMap());
    }

    /**
     * Replaces a legacy instance whose serialized stream has no current superclass state.
     *
     * <p>Serializable constructors do not initialize those absent fields. A new replacement
     * guarantees an empty assignment map and keeps the legacy marker used for restore logging.
     */
    private Object readResolve() {
        return new RabbitmqSourceState();
    }
}
