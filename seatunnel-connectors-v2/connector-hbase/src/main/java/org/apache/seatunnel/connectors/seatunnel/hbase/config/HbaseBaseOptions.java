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

package org.apache.seatunnel.connectors.seatunnel.hbase.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class HbaseBaseOptions {

    public static final Option<String> ZOOKEEPER_QUORUM =
            Options.key("zookeeper_quorum")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hbase zookeeper quorum");

    public static final Option<String> TABLE =
            Options.key("table").stringType().noDefaultValue().withDescription("Hbase table name");

    public static final Option<List<String>> ROWKEY_COLUMNS =
            Options.key("rowkey_column")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Hbase rowkey column");
}
