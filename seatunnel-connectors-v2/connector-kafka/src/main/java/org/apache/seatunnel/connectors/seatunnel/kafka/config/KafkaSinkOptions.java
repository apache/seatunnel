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

package org.apache.seatunnel.connectors.seatunnel.kafka.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class KafkaSinkOptions extends KafkaBaseOptions {

    public static final Option<Integer> PARTITION =
            Options.key("partition")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "We can specify the partition, all messages will be sent to this partition.");

    public static final Option<List<String>> ASSIGN_PARTITIONS =
            Options.key("assign_partitions")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "We can decide which partition to send based on the content of the message. "
                                    + "The function of this parameter is to distribute information.");

    public static final Option<List<String>> PARTITION_KEY_FIELDS =
            Options.key("partition_key_fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Configure which fields are used as the key of the kafka message.");

    public static final Option<KafkaSemantics> SEMANTICS =
            Options.key("semantics")
                    .enumType(KafkaSemantics.class)
                    .defaultValue(KafkaSemantics.NON)
                    .withDescription(
                            "Semantics that can be chosen EXACTLY_ONCE/AT_LEAST_ONCE/NON, default NON.");

    public static final Option<String> TRANSACTION_PREFIX =
            Options.key("transaction_prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "If semantic is specified as EXACTLY_ONCE, the producer will write all messages in a Kafka transaction. "
                                    + "Kafka distinguishes different transactions by different transactionId. "
                                    + "This parameter is prefix of kafka transactionId, make sure different job use different prefix.");
}
