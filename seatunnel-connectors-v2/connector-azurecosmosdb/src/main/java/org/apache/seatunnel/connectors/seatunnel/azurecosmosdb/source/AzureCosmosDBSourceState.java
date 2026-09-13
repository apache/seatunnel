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

package org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.source;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class AzureCosmosDBSourceState implements Serializable {

    private static final long serialVersionUID = -4911932288415348743L;

    private final boolean shouldEnumerate;
    private final Map<Integer, List<AzureCosmosDBSourceSplit>> pendingSplits;

    public AzureCosmosDBSourceState(
            boolean shouldEnumerate, Map<Integer, List<AzureCosmosDBSourceSplit>> pendingSplits) {
        this.shouldEnumerate = shouldEnumerate;
        this.pendingSplits = pendingSplits;
    }

    public boolean isShouldEnumerate() {
        return shouldEnumerate;
    }

    public Map<Integer, List<AzureCosmosDBSourceSplit>> getPendingSplits() {
        return pendingSplits;
    }
}
