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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source.event;

import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.table.catalog.TablePath;

import lombok.Getter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Read-only JDBC table-completion probe. Retrying it never decrements split counts. */
public final class JdbcTableCloseStatus {
    private JdbcTableCloseStatus() {}

    @Getter
    public static final class Request implements SourceEvent {
        private static final long serialVersionUID = 1L;
        private final UUID requestId;
        private final Set<TablePath> tables;

        public Request(UUID requestId, Set<TablePath> tables) {
            this.requestId = requestId;
            this.tables = new HashSet<>(tables);
        }
    }

    @Getter
    public static final class Reply implements SourceEvent {
        private static final long serialVersionUID = 1L;
        private final UUID requestId;
        private final Map<TablePath, Integer> finishedTables;

        public Reply(UUID requestId, Map<TablePath, Integer> finishedTables) {
            this.requestId = requestId;
            this.finishedTables = new HashMap<>(finishedTables);
        }
    }
}
