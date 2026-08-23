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

package org.apache.seatunnel.engine.common.job;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * HA journal entry for a member removal that may outlive the active coordinator which observed it.
 */
@AllArgsConstructor
@Data
@NoArgsConstructor
public class DirtyJobMemberEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    // Global cluster watermark, or -1 when only local fallback evidence exists.
    private long sequence;
    // Stable Hazelcast identity used for idempotent journal updates.
    private String memberUuid;
    // Diagnostic host used to match persisted task assignments.
    private String memberHost;
    // Diagnostic port used with the host for exact assignment matching.
    private int memberPort;
    // Original observation time retained across active-master replay.
    private long eventTime;

    /**
     * Returns the diagnostic address without using it as the member identity.
     *
     * @return host and port used only for affected-task matching and diagnostics
     */
    public String getAddressText() {
        return memberHost + ":" + memberPort;
    }
}
