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

package org.apache.seatunnel.connectors.seatunnel.datahub.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class DataHubConfig {

    public static Option<String> ENDPOINT = Options.key("endpoint")
        .stringType()
        .noDefaultValue()
        .withDescription("Your DataHub endpoint start with http");

    public static Option<String> ACCESS_ID = Options.key("accessId")
        .stringType()
        .noDefaultValue()
        .withDescription("Your DataHub accessId which cloud be access from Alibaba Cloud");

    public static Option<String> ACCESS_KEY = Options.key("accessKey")
        .stringType()
        .noDefaultValue()
        .withDescription("Your DataHub accessKey which cloud be access from Alibaba Cloud");

    public static Option<String> PROJECT = Options.key("project")
        .stringType()
        .noDefaultValue()
        .withDescription("Your DataHub project which is created in Alibaba Cloud");

    public static Option<String> TOPIC = Options.key("topic")
        .stringType()
        .noDefaultValue()
        .withDescription("Your DataHub topic which is created in Alibaba Cloud");

    public static Option<Integer> TIMEOUT = Options.key("timeout")
        .intType()
        .noDefaultValue()
        .withDescription("The max connection timeout");

    public static Option<Integer> RETRY_TIMES = Options.key("retryTimes")
        .intType()
        .noDefaultValue()
        .withDescription("The max retry times when your client put record failed");
}
