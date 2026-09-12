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

package org.apache.seatunnel.api.table.schema;

/** Policy values for handling schema change events emitted by CDC sources. */
public enum SchemaChangeBehavior {
    /**
     * Fails the job as soon as a schema change event is observed, before downstream schema
     * coordination or sink-side schema mutation is attempted.
     */
    STRICT,

    /**
     * Forwards supported schema change events through the normal schema coordination path.
     * Unsupported event types, unsupported sink capabilities, and sink-side apply failures are
     * fatal.
     */
    EVOLVE,

    /**
     * Drops comment-only schema change events before downstream schema coordination. Row-layout
     * changes fail because dropping them would leave decoded rows inconsistent with the runtime
     * schema. Configuration values are parsed case-insensitively.
     */
    IGNORE
}
