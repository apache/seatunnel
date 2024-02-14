/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hbase.source;

import org.apache.seatunnel.api.source.SourceSplit;

public class HbaseSourceSplit implements SourceSplit {
    public static final String HBASE_SOURCE_SPLIT_PREFIX = "hbase_source_split_";
    private String splitId;
    private byte[] startRow;
    private byte[] endRow;

    public HbaseSourceSplit(int splitId) {
        this.splitId = HBASE_SOURCE_SPLIT_PREFIX + splitId;
    }

    public HbaseSourceSplit(int splitId, byte[] startRow, byte[] endRow) {
        this.splitId = HBASE_SOURCE_SPLIT_PREFIX + splitId;
        this.startRow = startRow;
        this.endRow = endRow;
    }

    @Override
    public String toString() {
        return String.format("{\"split_id\":\"%s\"}", splitId);
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public byte[] getStartRow() {
        return startRow;
    }

    public byte[] getEndRow() {
        return endRow;
    }
}
