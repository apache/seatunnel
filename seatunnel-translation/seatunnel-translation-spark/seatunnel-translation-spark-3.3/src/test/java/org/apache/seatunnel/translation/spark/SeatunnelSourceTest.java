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

import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.connectors.seatunnel.fake.source.FakeSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SeatunnelSourceTest {

    private static final String RESULT_TABLE_NAME = "result_table_name";

    private static final String PLUGIN_NAME = "plugin_name";

    private static final String FIELD_NAME = "field_name";

    public void testBatchRead() {
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .getOrCreate();
        Config config = ConfigFactory.empty()
                .withValue(RESULT_TABLE_NAME, ConfigValueFactory.fromAnyRef("fake"))
                .withValue(PLUGIN_NAME, ConfigValueFactory.fromAnyRef("FakeSource"))
                .withValue(FIELD_NAME, ConfigValueFactory.fromAnyRef("name,age,timestamp"));
        SeaTunnelContext seaTunnelContext = SeaTunnelContext.getContext();
        seaTunnelContext.setJobMode(JobMode.BATCH);
        SeaTunnelSource<?, ?, ?> source = new FakeSource(config, seaTunnelContext);
        Dataset<Row> dataset = sparkSession.read()
                .format("org.apache.seatunnel.translation.spark.SeatunnelSource")
                .option(Constants.SOURCE_SERIALIZATION, SerializationUtils.objectToString(source))
                .load();
        assert !dataset.isEmpty();
        sparkSession.stop();
    }
}
