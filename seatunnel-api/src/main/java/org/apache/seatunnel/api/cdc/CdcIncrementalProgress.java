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

package org.apache.seatunnel.api.cdc;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

/** Incremental log or cursor progress reported by a CDC source. */
@Getter
@ToString
@EqualsAndHashCode
public class CdcIncrementalProgress implements Serializable {
    private static final long serialVersionUID = 1L;

    private final CdcProgressPosition currentConsumedPosition;
    private final CdcProgressPosition sourceHighWatermark;
    private final Long normalizedLag;
    private final CdcProgressLagUnit lagUnit;
    private final Long lastSourceEventTime;
    private final Long lastConsumedEventTime;
    private final long lastProgressTime;
    private final CdcProgressSupportLevel supportLevel;

    @Builder
    public CdcIncrementalProgress(
            CdcProgressPosition currentConsumedPosition,
            CdcProgressPosition sourceHighWatermark,
            Long normalizedLag,
            CdcProgressLagUnit lagUnit,
            Long lastSourceEventTime,
            Long lastConsumedEventTime,
            long lastProgressTime,
            CdcProgressSupportLevel supportLevel) {
        this.currentConsumedPosition =
                currentConsumedPosition == null
                        ? CdcProgressPosition.empty()
                        : currentConsumedPosition;
        this.sourceHighWatermark =
                sourceHighWatermark == null ? CdcProgressPosition.empty() : sourceHighWatermark;
        this.normalizedLag = normalizedLag;
        this.lagUnit = lagUnit == null ? CdcProgressLagUnit.UNKNOWN : lagUnit;
        this.lastSourceEventTime = lastSourceEventTime;
        this.lastConsumedEventTime = lastConsumedEventTime;
        this.lastProgressTime = lastProgressTime;
        this.supportLevel =
                supportLevel == null ? CdcProgressSupportLevel.UNSUPPORTED : supportLevel;
    }
}
