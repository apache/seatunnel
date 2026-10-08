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

package org.apache.seatunnel.engine.server.savepoint.serialization;

/**
 * Raised when a savepoint bundle cannot be read or restored because of a version or integrity
 * problem. The message always carries the savepoint id, the offending format version and the
 * supported version range so users get an actionable error instead of a serialization stack trace.
 */
public class SavepointIncompatibleException extends RuntimeException {

    private final String savepointId;
    private final int formatVersion;
    private final String reason;

    public SavepointIncompatibleException(String savepointId, int formatVersion, String reason) {
        super(
                String.format(
                        "Savepoint %s (format version %d) cannot be read: %s",
                        savepointId, formatVersion, reason));
        this.savepointId = savepointId;
        this.formatVersion = formatVersion;
        this.reason = reason;
    }

    public String getSavepointId() {
        return savepointId;
    }

    public int getFormatVersion() {
        return formatVersion;
    }

    public String getReason() {
        return reason;
    }
}
