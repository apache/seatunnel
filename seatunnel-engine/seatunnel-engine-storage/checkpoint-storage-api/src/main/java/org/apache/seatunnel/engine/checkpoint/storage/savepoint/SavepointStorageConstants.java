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

package org.apache.seatunnel.engine.checkpoint.storage.savepoint;

/** Stable constants of the savepoint storage format. */
public final class SavepointStorageConstants {

    /** Root directory (relative to the configured checkpoint namespace). */
    public static final String SAVEPOINT_ROOT_DIR = "savepoint";

    /** Directory holding in-flight savepoint attempts; never returned by list APIs. */
    public static final String STAGING_DIR = ".staging";

    /** Name of the commit marker file inside a final savepoint directory. */
    public static final String META_FILE_NAME = "_metadata.ser";

    /** Name of the wire format this savepoint format family uses. */
    public static final String FORMAT_NAME = "engine-wire";

    /** Current savepoint bundle format version; only increments. */
    public static final int FORMAT_VERSION = 1;

    /** Payload format of v1 savepoints (engine-wire-v1). */
    public static final String PAYLOAD_FORMAT_V1 = "engine-wire-v1";

    private SavepointStorageConstants() {}
}
