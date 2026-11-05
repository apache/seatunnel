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
package org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.source;

import lombok.Getter;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Enumerator checkpoint state containing only splits not yet assigned to a reader. */
@Getter
public class AzureEventHubsSourceState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Set<AzureEventHubsSourceSplit> pendingSplits;

    public AzureEventHubsSourceState(Set<AzureEventHubsSourceSplit> pendingSplits) {
        this.pendingSplits = Collections.unmodifiableSet(new HashSet<>(pendingSplits));
    }
}
