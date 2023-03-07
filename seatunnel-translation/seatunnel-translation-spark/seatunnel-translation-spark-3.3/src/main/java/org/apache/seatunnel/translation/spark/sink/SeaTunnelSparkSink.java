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

package org.apache.seatunnel.translation.spark.sink;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

/** SeaTunnel sink class of Spark 3+, can be used as sink */
public class SeaTunnelSparkSink implements DataSourceRegister, TableProvider {

    private static final String SINK_NAME = "SeaTunnelSink";

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return null;
    }

    @Override
    public Table getTable(
            StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new SeaTunnelSinkTable(properties);
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }

    @Override
    public String shortName() {
        return SINK_NAME;
    }
}
