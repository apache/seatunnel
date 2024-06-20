/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.config.sql.utils;

public class Constant {
    public static final String SQL_FILE_EXT = "sql";

    public static final String SQL_ANNOTATION_PREFIX2 = "--";

    public static final String SQL_ANNOTATION_PREFIX = "/*";

    public static final String SQL_CONFIG_ANNOTATION_PREFIX = SQL_ANNOTATION_PREFIX + " config";

    public static final String SQL_ANNOTATION_SUFFIX = "*/";

    public static final String SQL_DELIMITER = ";";

    public static final String OPTION_DELIMITER = ",'";

    public static final String OPTION_KV_DELIMITER = "=";

    public static final String OPTION_TABLE_TYPE_KEY = "type";

    public static final String OPTION_TABLE_TYPE_SOURCE = "source";

    public static final String OPTION_TABLE_TYPE_SINK = "sink";

    public static final String OPTION_TABLE_TYPE_TRANSFORM = "transform";

    public static final String OPTION_TABLE_CONNECTOR_KEY = "connector";

    public static final String OPTION_SOURCE_TABLE_NAME_KEY = "source_table_name";

    public static final String OPTION_RESULT_TABLE_NAME_KEY = "result_table_name";

    public static final String OPTION_SINGLE_QUOTES = "'";

    public static final String OPTION_DOUBLE_SINGLE_QUOTES = "''";

    public static final String TEMP_TABLE_SUFFIX = "__temp";
}
