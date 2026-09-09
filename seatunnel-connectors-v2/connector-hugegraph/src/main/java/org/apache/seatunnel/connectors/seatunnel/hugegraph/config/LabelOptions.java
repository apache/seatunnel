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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import java.util.Map;

/**
 * Optional HugeGraph label attributes applied at schema-creation time (TTL, TTL start-time
 * property, label-index toggle, and user-defined metadata). All fields are nullable; a null field
 * means "leave at the HugeGraph server default".
 */
public class LabelOptions {

    private final Long ttl;
    private final String ttlStartTime;
    private final Boolean enableLabelIndex;
    private final Map<String, Object> userdata;

    public LabelOptions(
            Long ttl, String ttlStartTime, Boolean enableLabelIndex, Map<String, Object> userdata) {
        this.ttl = ttl;
        this.ttlStartTime = ttlStartTime;
        this.enableLabelIndex = enableLabelIndex;
        this.userdata = userdata;
    }

    public Long getTtl() {
        return ttl;
    }

    public String getTtlStartTime() {
        return ttlStartTime;
    }

    public Boolean getEnableLabelIndex() {
        return enableLabelIndex;
    }

    public Map<String, Object> getUserdata() {
        return userdata;
    }
}
