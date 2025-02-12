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

package org.apache.seatunnel.connectors.seatunnel.amazonsqs.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class AmazonSqsSourceOptions extends AmazonSqsBaseOptions {

    public static final Option<Boolean> DELETE_MESSAGE =
            Options.key("delete_message")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Delete the message after it is consumed if set true.");

    public static final Option<String> MESSAGE_GROUP_ID =
            Options.key("message_group_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The message group id of Amazon SQS Service");

    public static final Option<Boolean> DEBEZIUM_RECORD_INCLUDE_SCHEMA =
            Options.key("debezium_record_include_schema")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Does the debezium record carry a schema.");
}
