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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.source;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.TablePath;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode
public class MaxcomputeSourceSplit implements SourceSplit {
    private final long rowStart;
    private final long rowNum;
    private final TablePath tablePath;
    private final int index;

    public MaxcomputeSourceSplit(long rowStart, long rowNum, TablePath tablePath, int index) {
        this.rowStart = rowStart;
        this.rowNum = rowNum;
        this.tablePath = tablePath;
        this.index = index;
    }

    @Override
    public String splitId() {
        return tablePath.toString() + "_" + index;
    }
}
