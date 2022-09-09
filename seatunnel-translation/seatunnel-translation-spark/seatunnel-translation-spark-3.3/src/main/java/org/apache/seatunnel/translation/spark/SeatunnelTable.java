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

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.translation.spark.common.utils.TypeConverterUtils;
import org.apache.seatunnel.translation.spark.common.utils.Utils;
import org.apache.seatunnel.translation.spark.sink.SeatunnelWriteBuilder;
import org.apache.seatunnel.translation.spark.source.SeatunnelScanBuilder;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;
import java.util.Set;

public class SeatunnelTable implements Table, SupportsRead, SupportsWrite {

    private final Map<String, String> properties;

    public SeatunnelTable(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        String source = options.get(Constants.SOURCE_SERIALIZATION);
        SeaTunnelSource<SeaTunnelRow, ?, ?> seaTunnelSource = Utils.getSeaTunnelSource(source);
        int parallelism = Integer.parseInt(
                options.getOrDefault(Constants.SOURCE_PARALLELISM, "1"));
        return new SeatunnelScanBuilder(seaTunnelSource, parallelism);
    }

    @Override
    public String name() {
        return "SeaTunnel-Table";
    }

    @Override
    public StructType schema() {
        String source = properties.get(Constants.SOURCE_SERIALIZATION);
        String sink = properties.get(Constants.SINK);
        SeaTunnelDataType<SeaTunnelRow> seaTunnelDataType;
        if (StringUtils.isNotBlank(source)) {
            SeaTunnelSource<SeaTunnelRow, ?, ?> seaTunnelSource = Utils.getSeaTunnelSource(source);
            seaTunnelDataType = seaTunnelSource.getProducedType();
        } else if (StringUtils.isNotBlank(sink)) {
            SeaTunnelSink<SeaTunnelRow, ?, ?, ?> seaTunnelSink = SerializationUtils.stringToObject(sink);
            seaTunnelDataType = seaTunnelSink.getConsumedType();
        } else {
            throw new IllegalArgumentException(String.format("%s and %s can't be blank",
                    Constants.SOURCE_SERIALIZATION, Constants.SINK));
        }
        return (StructType) TypeConverterUtils.convert(seaTunnelDataType);
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Sets.newHashSet(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        SeaTunnelSink<SeaTunnelRow, ?, ?, ?> sink = SerializationUtils.stringToObject(
                info.options().get(Constants.SINK));
        return new SeatunnelWriteBuilder<>(sink);
    }
}
