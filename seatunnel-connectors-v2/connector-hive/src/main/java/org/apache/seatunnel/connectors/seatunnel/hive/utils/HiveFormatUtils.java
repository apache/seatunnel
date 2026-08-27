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

package org.apache.seatunnel.connectors.seatunnel.hive.utils;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

public class HiveFormatUtils {

    public static void configureStorageDescriptor(StorageDescriptor sd, String format) {
        format = format.toUpperCase();

        switch (format) {
            case "PARQUET":
                configureParquetFormat(sd);
                break;
            case "ORC":
                configureOrcFormat(sd);
                break;
            case "TEXTFILE":
                configureTextFileFormat(sd);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported table format: "
                                + format
                                + ". Supported formats: PARQUET, ORC, TEXTFILE");
        }
    }

    /** Configure Parquet format with default SNAPPY compression */
    private static void configureParquetFormat(StorageDescriptor sd) {
        sd.setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        sd.setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");

        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setSerializationLib(
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        sd.setSerdeInfo(serDeInfo);
    }

    /** Configure ORC format with default ZLIB compression */
    private static void configureOrcFormat(StorageDescriptor sd) {
        sd.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
        sd.setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");

        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setSerializationLib("org.apache.hadoop.hive.ql.io.orc.OrcSerde");
        sd.setSerdeInfo(serDeInfo);
    }

    /** Configure TextFile format with default GZIP compression */
    private static void configureTextFileFormat(StorageDescriptor sd) {
        sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
        sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");

        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        sd.setSerdeInfo(serDeInfo);
    }

    /** Get default table properties for the specified format */
    public static String getDefaultTableProperties(String format) {
        format = format.toUpperCase();

        switch (format) {
            case "PARQUET":
                return "'parquet.compression'='SNAPPY',\n  'created_by'='seatunnel'";
            case "ORC":
                return "'orc.compress'='ZLIB',\n  'created_by'='seatunnel'";
            case "TEXTFILE":
                return "'created_by'='seatunnel'";
            default:
                return "'created_by'='seatunnel'";
        }
    }

    /** Check if compression should be enabled for the format */
    public static boolean shouldEnableCompression(String format) {
        format = format.toUpperCase();
        // Enable compression for PARQUET and ORC, not for TEXTFILE by default
        return "PARQUET".equals(format) || "ORC".equals(format);
    }

    /** Validate if the format is supported */
    public static void validateFormat(String format) {
        if (format == null || format.trim().isEmpty()) {
            throw new IllegalArgumentException("Table format cannot be null or empty");
        }

        format = format.toUpperCase();
        if (!"PARQUET".equals(format) && !"ORC".equals(format) && !"TEXTFILE".equals(format)) {
            throw new IllegalArgumentException(
                    "Unsupported table format: "
                            + format
                            + ". Supported formats: PARQUET, ORC, TEXTFILE");
        }
    }
}
