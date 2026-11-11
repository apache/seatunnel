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

package org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlIncrementalSourceOptions;

/**
 * Options of the {@link OceanBaseIncrementalSource}.
 *
 * <p>The connector reuses the MySQL CDC runtime, so it inherits the MySQL CDC options and only adds
 * the OceanBase-specific constants on top.
 */
public class OceanBaseIncrementalSourceOptions extends MySqlIncrementalSourceOptions {

    /**
     * The only compatible mode supported by the OceanBase CDC source. The source reuses the MySQL
     * CDC runtime, so the mode is fixed to MySQL compatible mode for now.
     */
    public static final String MYSQL_COMPATIBLE_MODE = "mysql";
}
