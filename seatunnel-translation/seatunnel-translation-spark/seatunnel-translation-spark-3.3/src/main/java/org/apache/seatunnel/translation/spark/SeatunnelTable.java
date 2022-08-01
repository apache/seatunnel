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

package org.apache.seatunnel.translation.spark;

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.spark.common.utils.TypeConverterUtils;
import org.apache.seatunnel.translation.spark.source.SeatunnelScanBuilder;

import com.google.common.collect.Sets;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Set;

public class SeatunnelTable implements Table, SupportsRead {

    private final SeaTunnelSource<SeaTunnelRow, ?, ?> source;
    private final Integer parallelism;

    public SeatunnelTable(SeaTunnelSource<SeaTunnelRow, ?, ?> source, Integer parallelism) {
        this.source = source;
        this.parallelism = parallelism;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new SeatunnelScanBuilder(source, parallelism);
    }

    @Override
    public String name() {
        return "SeaTunnel-Table";
    }

    @Override
    public StructType schema() {
        return (StructType) TypeConverterUtils.convert(source.getProducedType());
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Sets.newHashSet(TableCapability.BATCH_READ);
    }
}
