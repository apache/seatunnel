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
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.translation.spark.common.sink.SparkSinkService;
import org.apache.seatunnel.translation.spark.common.utils.SparkSourceConstants;
import org.apache.seatunnel.translation.spark.common.utils.Utils;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class SeatunnelSource implements DataSourceRegister, TableProvider, SparkSinkService {

    @Override
    public String shortName() {
        return SparkSourceConstants.SEA_TUNNEL_SOURCE_NAME;
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return null;
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new SeatunnelTable(properties);
    }

    private SeaTunnelSource<SeaTunnelRow, ?, ?> getSeaTunnelSource(Map<String, String> options) {
        String source = options.get(Constants.SOURCE_SERIALIZATION);
        return Utils.getSeaTunnelSource(source);
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }
}
