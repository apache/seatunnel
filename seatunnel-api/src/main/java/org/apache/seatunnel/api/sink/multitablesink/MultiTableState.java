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

package org.apache.seatunnel.api.sink.multitablesink;

import org.apache.seatunnel.api.common.multitable.MultiTableFailedTable;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Getter
@AllArgsConstructor
public class MultiTableState implements Serializable {
    // Keep the generated UID from the pre-failedTables class shape so old checkpoints restore.
    private static final long serialVersionUID = 5992121739651030596L;

    private Map<SinkIdentifier, List<?>> states;
    private List<MultiTableFailedTable> failedTables;

    public MultiTableState(Map<SinkIdentifier, List<?>> states) {
        this(states, Collections.emptyList());
    }

    public List<MultiTableFailedTable> getFailedTables() {
        return failedTables == null ? Collections.emptyList() : failedTables;
    }
}
