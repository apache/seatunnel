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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Connector-native CDC position preserved as stable string key/value pairs. */
@Getter
@ToString
@EqualsAndHashCode
public class CdcProgressPosition implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final CdcProgressPosition EMPTY =
            new CdcProgressPosition(Collections.emptyMap());

    private final Map<String, String> values;

    public CdcProgressPosition(Map<String, String> values) {
        if (values == null || values.isEmpty()) {
            this.values = Collections.emptyMap();
        } else {
            this.values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
        }
    }

    public static CdcProgressPosition empty() {
        return EMPTY;
    }

    public static CdcProgressPosition of(Map<String, String> values) {
        return new CdcProgressPosition(values);
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }
}
