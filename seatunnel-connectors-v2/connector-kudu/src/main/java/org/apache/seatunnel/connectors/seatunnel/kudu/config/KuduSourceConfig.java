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

package org.apache.seatunnel.connectors.seatunnel.kudu.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.io.Serializable;

public class KuduSourceConfig implements Serializable {

    public static final Option<String> KUDU_MASTER =
        Options.key("kudu_master")
            .stringType()
            .noDefaultValue()
            .withDescription("Kudu master address");

    public static final Option<String> TABLE_NAME =
        Options.key("kudu_table")
            .stringType()
            .noDefaultValue()
            .withDescription("Kudu table name");

    public static final Option<String> COLUMNS_LIST =
        Options.key("columnsList")
            .stringType()
            .noDefaultValue()
            .withDescription("Specifies the column names of the table");

}
