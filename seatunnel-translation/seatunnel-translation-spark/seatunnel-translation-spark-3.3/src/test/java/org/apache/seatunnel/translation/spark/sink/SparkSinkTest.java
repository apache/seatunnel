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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class SparkSinkTest {

    @Test
    public void testSparkSinkWriteDataWithCopy() {
        // We should make sure that the data is written to the sink with copy.
        SparkSession spark =
                SparkSession.builder()
                        .master("local")
                        .appName("testSparkSinkWriteDataWithCopy")
                        .getOrCreate();
        Dataset<Row> dataset =
                spark.createDataFrame(
                        Arrays.asList(
                                new GenericRow(new Object[] {"fanjia"}),
                                new GenericRow(new Object[] {"hailin"}),
                                new GenericRow(new Object[] {"wenjun"})),
                        new StructType().add("name", "string"));
        SparkSinkInjector.inject(dataset.write(), new SeaTunnelSinkWithBuffer())
                .option("checkpointLocation", "/tmp")
                .mode(SaveMode.Append)
                .save();
        spark.close();
    }
}
