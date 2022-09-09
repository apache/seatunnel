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

package org.apache.seatunnel.translation.spark.common.sink;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.utils.SerializationUtils;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

public class SparkSinkInjector {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkSinkInjector.class);

    private static final String SPARK_SINK_CLASS_NAME = loadSparkSink();

    private static String loadSparkSink() {
        Iterator<SparkSinkService> iterator = ServiceLoader
                .load(SparkSinkService.class, Thread.currentThread().getContextClassLoader()).iterator();
        List<SparkSinkService> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        if (list.isEmpty()) {
            throw new IllegalArgumentException("Can't found implementation of SparkSinkService.");
        }
        if (list.size() > 1) {
            LOGGER.warn(String.format("Found %s implementation of SparkSinkService, we will use the first.", list.size()));
        }
        return list.get(0).getClass().getName();
    }

    public static DataStreamWriter<Row> inject(DataStreamWriter<Row> dataset, SeaTunnelSink<?, ?, ?, ?> sink) {
        return dataset.format(SPARK_SINK_CLASS_NAME)
            .outputMode(OutputMode.Append())
            .option(Constants.SINK, SerializationUtils.objectToString(sink));
    }

    public static DataFrameWriter<Row> inject(DataFrameWriter<Row> dataset, SeaTunnelSink<?, ?, ?, ?> sink) {
        return dataset.format(SPARK_SINK_CLASS_NAME)
            .option(Constants.SINK, SerializationUtils.objectToString(sink));
    }

}
