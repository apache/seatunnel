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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.split;

import org.apache.seatunnel.api.source.SourceSplit;

import org.tikv.kvproto.Coprocessor;
import org.tikv.shade.com.google.protobuf.ByteString;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class TiDBSourceSplit implements SourceSplit {

    private static final long serialVersionUID = -9043797960947110643L;
    private String database;
    private String table;
    private Coprocessor.KeyRange keyRange;
    private long resolvedTs;
    private ByteString snapshotStart;
    private boolean snapshotCompleted;

    /**
     * Get the split id of this source split.
     *
     * @return id of this source split.
     */
    @Override
    public String splitId() {
        return String.format(
                "%s:%s:%s-%s", database, table, keyRange.getStart(), keyRange.getEnd());
    }

    @Override
    public String toString() {
        return String.format(
                "TiDBSourceSplit: %s.%s,start=%s,end=%s",
                getDatabase(), getTable(), getKeyRange().getStart(), getKeyRange().getEnd());
    }
}
