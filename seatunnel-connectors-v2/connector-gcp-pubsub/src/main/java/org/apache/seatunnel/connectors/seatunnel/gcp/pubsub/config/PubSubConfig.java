/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.gcp.pubsub.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class PubSubConfig {

    // --------------------------------------------------------------------------------------------
    // The configuration for Google Cloud Service.
    // --------------------------------------------------------------------------------------------
    public static final Option<String> SERVICE_ACCOUNT_KEYS =
            Options.key("service_account_keys")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Google cloud login service account key");

    // --------------------------------------------------------------------------------------------
    // The configuration for PubSub Subscription.
    // --------------------------------------------------------------------------------------------
    public static final Option<String> PROJECT_ID =
            Options.key("project_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The project id of Google pubsub.");

    public static final Option<String> SUBSCRIPTION_ID =
            Options.key("subscription_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The subscription id of Google pubsub.");

    public static final Option<String> TOPIC =
            Options.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The topic of Google pubsub.");

    public static final Option<String> SUBSCRIPTION_TYPE =
            Options.key("subscription_type")
                    .stringType()
                    .defaultValue(SubscriptionType.PULL.name())
                    .withDescription("The subscript type of Google Pubsub.");

    public static final Option<Long> SUBSCRIPT_INTERVAL_MILLIS =
            Options.key("subscript_interval_millis")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("");

    public static final Option<String> FORMAT_ERROR_HANDLE =
            Options.key("format_error_handle")
                    .stringType()
                    .defaultValue(MessageFormatErrorHandleWay.FAIL.name())
                    .withDescription(
                            "The processing method of data format error. The default value is fail, and the optional value is (fail, skip). "
                            + "When fail is selected, data format error will block and an exception will be thrown. "
                            + "When skip is selected, data format error will skip this line data.");

    public enum SubscriptionType {
        PULL,
        PUSH,
        BIG_QUERY,
    }

    public enum MessageFormatErrorHandleWay {
        FAIL,
        SKIP,
    }
}
