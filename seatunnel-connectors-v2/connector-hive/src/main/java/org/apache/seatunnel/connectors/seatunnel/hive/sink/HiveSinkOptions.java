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

package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveOptions;

public class HiveSinkOptions extends HiveOptions {

    public static final Option<Boolean> ABORT_DROP_PARTITION_METADATA =
            Options.key("abort_drop_partition_metadata")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Flag to decide whether to drop partition metadata from Hive Metastore during an abort operation. Note: this only affects the metadata in the metastore, the data in the partition will always be deleted(data generated during the synchronization process).");

    public static final Option<Boolean> OVERWRITE =
            Options.key("overwrite")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Flag to decide whether to use overwrite mode when inserting data into Hive. If set to true, for non-partitioned tables, the existing data in the table will be deleted before inserting new data. For partitioned tables, the data in the relevant partition will be deleted before inserting new data.");

    // SaveMode related options
    public static final Option<SchemaSaveMode> SCHEMA_SAVE_MODE =
            Options.key("schema_save_mode")
                    .enumType(SchemaSaveMode.class)
                    .defaultValue(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST)
                    .withDescription(
                            "Schema save mode for auto table creation. "
                                    + "CREATE_SCHEMA_WHEN_NOT_EXIST: Create table when not exists (default). "
                                    + "RECREATE_SCHEMA: Drop and recreate table. "
                                    + "ERROR_WHEN_SCHEMA_NOT_EXIST: Throw error when table not exists. "
                                    + "IGNORE: Skip table creation.");

    public static final Option<DataSaveMode> DATA_SAVE_MODE =
            Options.key("data_save_mode")
                    .enumType(DataSaveMode.class)
                    .defaultValue(DataSaveMode.APPEND_DATA)
                    .withDescription("Data save mode. DROP_DATA behaves like overwrite=true.");

    public static final Option<String> SAVE_MODE_CREATE_TEMPLATE =
            Options.key("save_mode_create_template")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "We use templates to automatically create Hive tables, "
                                    + "which will create corresponding table creation statements based on the type of upstream data and schema type, "
                                    + "and the default template can be modified according to the situation. "
                                    + "Available template variables: ${database}, ${table}, ${rowtype_fields}, ${rowtype_partition_fields}, ${table_location}.");
}
