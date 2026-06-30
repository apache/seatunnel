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

package org.apache.seatunnel.connectors.seatunnel.bigtable.source;

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.Serializable;

public class BigtableSourceSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;
    public static final String SPLIT_PREFIX = "bigtable_source_split_";

    private final String splitId;
    /** Inclusive start row key (empty means the table start). */
    private final String startRowKey;
    /** Exclusive end row key (empty means the table end). */
    private final String endRowKey;

    public BigtableSourceSplit(int splitIndex, String startRowKey, String endRowKey) {
        this.splitId = SPLIT_PREFIX + splitIndex;
        this.startRowKey = startRowKey;
        this.endRowKey = endRowKey;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public String getStartRowKey() {
        return startRowKey;
    }

    public String getEndRowKey() {
        return endRowKey;
    }

    @Override
    public String toString() {
        return String.format(
                "{\"split_id\":\"%s\", \"start\":\"%s\", \"end\":\"%s\"}",
                splitId, startRowKey, endRowKey);
    }
}
