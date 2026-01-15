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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;

/** Compatibility layer for Hive options and inherited file source options. */
public class HiveOptions extends FileBaseSourceOptions {

    public static final Option<String> TABLE_NAME = HiveConfig.TABLE_NAME;

    public static final Option<String> METASTORE_URI = HiveConfig.METASTORE_URI;

    public static final Option<Boolean> USE_REGEX = HiveConfig.USE_REGEX;

    public static final Option<String> HIVE_SITE_PATH = HiveConfig.HIVE_SITE_PATH;
}
