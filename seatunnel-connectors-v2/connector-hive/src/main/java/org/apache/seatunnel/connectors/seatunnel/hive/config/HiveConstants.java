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

package org.apache.seatunnel.connectors.seatunnel.hive.config;

public class HiveConstants {

    public static final String CONNECTOR_NAME = "Hive";

    public static final String TEXT_INPUT_FORMAT_CLASSNAME =
            "org.apache.hadoop.mapred.TextInputFormat";
    public static final String TEXT_OUTPUT_FORMAT_CLASSNAME =
            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
    public static final String PARQUET_INPUT_FORMAT_CLASSNAME =
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    public static final String PARQUET_OUTPUT_FORMAT_CLASSNAME =
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
    public static final String ORC_INPUT_FORMAT_CLASSNAME =
            "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    public static final String ORC_OUTPUT_FORMAT_CLASSNAME =
            "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
}
