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

/** Details explaining why a CDC progress snapshot is marked as stalled. */
@Getter
@ToString
@EqualsAndHashCode
public class CdcStalledStatus implements Serializable {
    private static final long serialVersionUID = 1L;

    private final CdcProgressStalledSource source;
    private final String reason;
    private final Long timeoutMillis;

    @Builder
    public CdcStalledStatus(CdcProgressStalledSource source, String reason, Long timeoutMillis) {
        this.source = source;
        this.reason = reason;
        this.timeoutMillis = timeoutMillis;
    }
}
