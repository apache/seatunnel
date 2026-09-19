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

package org.apache.seatunnel.engine.server.task.source;

/** Version-stable command kinds used by the managed Source transport. */
public enum SourceCommandKind {
    READER_EPOCH_START(1, true),
    ASSIGN_SPLITS(2, false),
    NO_MORE_SPLITS(3, true),
    SOURCE_EVENT(4, false),
    BARRIER(5, true),
    CHECKPOINT_COMPLETE(6, true),
    CHECKPOINT_ABORTED(7, true),
    CHECKPOINT_END(8, true),
    PREPARE_CLOSE(9, true),
    CANCEL(10, true),
    REQUEST_SPLIT(11, false),
    READER_SOURCE_EVENT(12, false),
    READER_FINISHED(13, true),
    COMMAND_APPLIED(14, false),
    READER_CHECKPOINT_REPORT(15, true),
    RESTORED_SPLITS(16, true);

    private final int code;
    private final boolean reservedControl;

    SourceCommandKind(int code, boolean reservedControl) {
        this.code = code;
        this.reservedControl = reservedControl;
    }

    public int getCode() {
        return code;
    }

    public boolean isReservedControl() {
        return reservedControl;
    }

    public static SourceCommandKind fromCode(int code) {
        for (SourceCommandKind kind : values()) {
            if (kind.code == code) {
                return kind;
            }
        }
        throw new IllegalArgumentException("Unknown Source command kind code " + code);
    }
}
