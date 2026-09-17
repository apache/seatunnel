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

package org.apache.seatunnel.connectors.seatunnel.jdbc.state;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceSplit;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Enumerator checkpoint state for JDBC source split planning.
 *
 * <p>{@code generatorState} may be null when restoring older checkpoints that only persisted
 * pending tables/splits, or when no table is mid-generation.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JdbcSourceState implements Serializable {
    // Keep the original UID so pre-PR checkpoints deserialize with generatorState=null.
    private static final long serialVersionUID = -6441009212721284346L;
    private List<TablePath> pendingTables;
    private Map<Integer, List<JdbcSourceSplit>> pendingSplits;
    /** Cursor for the table currently being lazily split; null if none. */
    private JdbcSplitGeneratorState generatorState;

    public JdbcSourceState(
            List<TablePath> pendingTables, Map<Integer, List<JdbcSourceSplit>> pendingSplits) {
        this(pendingTables, pendingSplits, null);
    }
}
