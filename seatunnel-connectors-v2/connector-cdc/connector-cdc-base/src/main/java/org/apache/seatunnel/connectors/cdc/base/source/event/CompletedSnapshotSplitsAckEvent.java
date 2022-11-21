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

package org.apache.seatunnel.connectors.cdc.base.source.event;

import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.IncrementalSourceEnumerator;
import org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceReader;

import lombok.Data;

import java.util.List;

/**
 * The {@link SourceEvent} that {@link IncrementalSourceEnumerator} sends to {@link
 * IncrementalSourceReader} to notify the completed snapshot splits has been received, i.e.
 * acknowledge for {@link CompletedSnapshotSplitsReportEvent}.
 */
@Data
public class CompletedSnapshotSplitsAckEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final List<String> completedSplits;

}
