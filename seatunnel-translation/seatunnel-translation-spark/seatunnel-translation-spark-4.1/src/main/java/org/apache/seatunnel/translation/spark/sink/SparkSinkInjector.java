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

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.utils.SerializationUtils;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;

public class SparkSinkInjector {

    private static final String SINK_NAME = SeaTunnelSink.class.getSimpleName();

    public static final String SINK_CATALOG_TABLE = "sink.catalog.table";

    public static final String JOB_ID = "jobId";

    public static final String PARALLELISM = "parallelism";

    public static DataStreamWriter<Row> inject(
            DataStreamWriter<Row> dataset,
            SeaTunnelSink<?, ?, ?, ?> sink,
            CatalogTable[] catalogTables,
            String applicationId,
            int parallelism) {
        return dataset.format(SINK_NAME)
                .outputMode(OutputMode.Append())
                .option(Constants.SINK_SERIALIZATION, SerializationUtils.objectToString(sink))
                // TODO this should require fetching the catalog table in sink
                .option(SINK_CATALOG_TABLE, SerializationUtils.objectToString(catalogTables))
                .option(JOB_ID, applicationId)
                .option(PARALLELISM, parallelism);
    }

    public static DataFrameWriter<Row> inject(
            DataFrameWriter<Row> dataset,
            SeaTunnelSink<?, ?, ?, ?> sink,
            CatalogTable[] catalogTables,
            String applicationId,
            int parallelism) {
        return dataset.format(SINK_NAME)
                .option(Constants.SINK_SERIALIZATION, SerializationUtils.objectToString(sink))
                // TODO this should require fetching the catalog table in sink
                .option(SINK_CATALOG_TABLE, SerializationUtils.objectToString(catalogTables))
                .option(JOB_ID, applicationId)
                .option(PARALLELISM, parallelism);
    }
}
