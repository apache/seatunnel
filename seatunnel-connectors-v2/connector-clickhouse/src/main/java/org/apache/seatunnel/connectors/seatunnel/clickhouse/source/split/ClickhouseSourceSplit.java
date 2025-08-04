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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.source.split;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.source.ClickhousePart;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Setter;

import java.util.List;

@Data
@AllArgsConstructor
public class ClickhouseSourceSplit implements SourceSplit {

    private static final long serialVersionUID = 8626697814676246066L;

    private final TablePath tablePath;
    private final TablePath configTablePath;
    private final List<ClickhousePart> parts;
    private final Shard shard;
    private final String splitQuery;
    @Setter private int sqlOffset;

    private final String splitId;

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return "ClickhouseSourceSplit{"
                + "tablePath='"
                + tablePath
                + "'"
                + ", configTablePath='"
                + configTablePath
                + "'"
                + ", parts='"
                + parts
                + "'"
                + ", shard='"
                + shard
                + "'"
                + ", splitQuery='"
                + splitQuery
                + "'"
                + ", sqlOffset="
                + sqlOffset
                + ", splitId='"
                + splitId
                + "'"
                + "}";
    }
}
