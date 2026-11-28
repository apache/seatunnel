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

package org.apache.seatunnel.connectors.cdc.base.config;

import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

@EqualsAndHashCode
public final class StartupConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    @Getter private final StartupMode startupMode;
    private final String specificOffsetFile;
    private final Long specificOffsetPos;
    @Getter private final Long timestamp;
    private final Map<String, String> specificOffset;

    public StartupConfig(
            StartupMode startupMode,
            String specificOffsetFile,
            Long specificOffsetPos,
            Long timestamp) {
        this(startupMode, specificOffsetFile, specificOffsetPos, timestamp, null);
    }

    public StartupConfig(StartupMode startupMode, Map<String, String> specificOffset) {
        this(startupMode, null, null, null, specificOffset);
    }

    public StartupConfig(
            StartupMode startupMode,
            String specificOffsetFile,
            Long specificOffsetPos,
            Long timestamp,
            Map<String, String> specificOffset) {
        this.startupMode = startupMode;
        this.specificOffsetFile = specificOffsetFile;
        this.specificOffsetPos = specificOffsetPos;
        this.timestamp = timestamp;
        this.specificOffset = specificOffset == null ? null : new LinkedHashMap<>(specificOffset);
    }

    public Offset getStartupOffset(OffsetFactory offsetFactory) {
        switch (startupMode) {
            case EARLIEST:
                return offsetFactory.earliest();
            case LATEST:
                return offsetFactory.latest();
            case INITIAL:
            case SNAPSHOT_ONLY:
                return null;
            case COMMITTED_OFFSET:
                return offsetFactory.committedOffset();
            case SPECIFIC:
            case MIXED:
                if (specificOffset != null) {
                    return offsetFactory.specific(specificOffset);
                }
                return offsetFactory.specific(specificOffsetFile, specificOffsetPos);
            case TIMESTAMP:
                return offsetFactory.timestamp(timestamp);
            default:
                throw new IllegalArgumentException(
                        String.format("The %s mode is not supported.", startupMode));
        }
    }
}
