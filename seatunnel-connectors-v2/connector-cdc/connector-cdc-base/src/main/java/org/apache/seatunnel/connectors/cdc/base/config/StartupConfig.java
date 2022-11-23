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

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;

@AllArgsConstructor
@EqualsAndHashCode
public final class StartupConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    @Getter
    private final StartupMode startupMode;
    private final String specificOffsetFile;
    private final Long specificOffsetPos;
    private final Long timestamp;

    public Offset getStartupOffset(OffsetFactory offsetFactory) {
        switch (startupMode) {
            case EARLIEST:
                return offsetFactory.earliest();
            case LATEST:
                return offsetFactory.latest();
            case INITIAL:
                return null;
            case TIMESTAMP:
                return offsetFactory.timestamp(timestamp);
            default:
                throw new IllegalArgumentException(String.format("The %s mode is not supported.", startupMode));
        }
    }
}
