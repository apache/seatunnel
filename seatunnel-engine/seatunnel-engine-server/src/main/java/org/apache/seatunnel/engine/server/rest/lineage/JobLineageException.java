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

package org.apache.seatunnel.engine.server.rest.lineage;

import lombok.Getter;

/**
 * Raised when a job DAG snapshot cannot be projected into a lineage graph. The message is fixed per
 * reason and never contains topology names, table paths or configuration values.
 */
@Getter
public class JobLineageException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** Why the projection was rejected. */
    public enum Reason {
        /** The snapshot is missing required topology or is not a valid graph. */
        LINEAGE_UNAVAILABLE("A consistent lineage snapshot is not available for this job"),
        /** A structural, string or serialized-byte limit was exceeded. */
        LINEAGE_GRAPH_TOO_LARGE("The lineage graph exceeds the response limits");

        private final String message;

        Reason(String message) {
            this.message = message;
        }
    }

    private final Reason reason;

    public JobLineageException(Reason reason) {
        super(reason.message);
        this.reason = reason;
    }
}
