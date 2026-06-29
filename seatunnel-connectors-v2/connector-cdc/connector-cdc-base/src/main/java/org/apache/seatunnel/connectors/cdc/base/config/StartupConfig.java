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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@EqualsAndHashCode
public final class StartupConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    @Getter private final StartupMode startupMode;
    private final String specificOffsetFile;
    private final Long specificOffsetPos;
    private final Map<String, String> specificOffset;
    @Getter private final Long timestamp;

    public StartupConfig(
            StartupMode startupMode,
            String specificOffsetFile,
            Long specificOffsetPos,
            Long timestamp) {
        this(startupMode, specificOffsetFile, specificOffsetPos, null, timestamp);
    }

    public static StartupConfig specificOffset(Map<String, String> specificOffset) {
        return new StartupConfig(
                StartupMode.SPECIFIC, null, null, Objects.requireNonNull(specificOffset), null);
    }

    private StartupConfig(
            StartupMode startupMode,
            String specificOffsetFile,
            Long specificOffsetPos,
            Map<String, String> specificOffset,
            Long timestamp) {
        this.startupMode = startupMode;
        this.specificOffsetFile = specificOffsetFile;
        this.specificOffsetPos = specificOffsetPos;
        this.specificOffset = specificOffset == null ? null : new HashMap<>(specificOffset);
        this.timestamp = timestamp;
    }

    public Offset getStartupOffset(OffsetFactory offsetFactory) {
        switch (startupMode) {
            case EARLIEST:
                return offsetFactory.earliest();
            case LATEST:
                return offsetFactory.latest();
            case INITIAL:
                return null;
            case SPECIFIC:
                if (specificOffset != null) {
                    return offsetFactory.specific(new HashMap<>(specificOffset));
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
