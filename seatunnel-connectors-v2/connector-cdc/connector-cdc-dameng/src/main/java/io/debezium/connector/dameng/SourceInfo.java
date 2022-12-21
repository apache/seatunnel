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

package io.debezium.connector.dameng;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@NotThreadSafe
@Getter
@Setter
public class SourceInfo extends BaseSourceInfo {
    public static final String SCN_KEY = "scn";
    public static final String COMMIT_SCN_KEY = "commit_scn";
    public static final String EVENT_SERIAL_NO_KEY = "event_serial_no";

    private Scn scn;
    private Scn commitScn;
    private Long eventSerialNo;
    private Instant sourceTime;
    private Set<TableId> tableIds;

    public SourceInfo(DamengConnectorConfig config) {
        super(config);
    }

    public String tableSchema() {
        return tableIds.isEmpty() ? null
            : tableIds.stream()
            .filter(x -> x != null)
            .map(TableId::schema)
            .distinct()
            .collect(Collectors.joining(","));
    }

    public String table() {
        return tableIds.isEmpty() ? null
            : tableIds.stream()
            .filter(x -> x != null)
            .map(TableId::table)
            .collect(Collectors.joining(","));
    }

    public void tableEvent(Set<TableId> tableIds) {
        this.tableIds = new HashSet<>(tableIds);
    }

    public void tableEvent(TableId tableId) {
        this.tableIds = Collections.singleton(tableId);
    }

    @Override
    protected Instant timestamp() {
        return sourceTime;
    }

    @Override
    protected String database() {
        return tableIds.iterator().next().catalog();
    }
}
