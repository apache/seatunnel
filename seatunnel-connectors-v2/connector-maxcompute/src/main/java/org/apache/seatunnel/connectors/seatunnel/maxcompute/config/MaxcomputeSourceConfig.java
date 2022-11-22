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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.io.Serializable;

public class MaxcomputeSourceConfig implements Serializable {

    public static final Option<String> ALIYUN_ODPS_ACCOUNTID =
        Options.key("aliyun_odps_accountid")
            .stringType()
            .noDefaultValue()
            .withDescription("aliyun.odps.accountId");

    public static final Option<String> ALIYUN_ODPS_ACCOUNTKEY =
        Options.key("aliyun_odps_accountkey")
            .stringType()
            .noDefaultValue()
            .withDescription("aliyun.odps.accountKey");

    public static final Option<String> ALIYUN_ODPS_ENDPOINT =
        Options.key("aliyun_odps_endpoint")
            .stringType()
            .noDefaultValue()
            .withDescription("aliyun.odps.endpoint");

    public static final Option<String> ALIYUN_ODPS_DEFAULTPROJECT =
            Options.key("aliyun_odps_defaultproject")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("aliyun.odps.defaultProject");

    public static final Option<String> MAXCOMPUTE_TABLE_NAME =
            Options.key("maxcompure_table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("maxcompure.table.name");

    public static final Option<String> MAXCOMPUTE_TABLE_PARTITION =
            Options.key("maxcompute_table_partition")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("maxcompute.table.partition");
}
