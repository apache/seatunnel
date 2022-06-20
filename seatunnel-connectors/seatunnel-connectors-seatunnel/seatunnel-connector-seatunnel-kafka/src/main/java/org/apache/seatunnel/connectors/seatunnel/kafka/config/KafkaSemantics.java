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

public enum KafkaSemantics {

    /**
     * At this semantics, we will directly send the message to kafka, the data may duplicat/lost
     * if job restart/retry or network error.
     */
    NON,

    /**
     * At this semantics, we will retry sending the message to kafka, if the response is not ack.
     */
    AT_LEAST_ONCE,

    /**
     * AT this semantics, we will use 2pc to guarantee the message is sent to kafka exactly once.
     */
    EXACTLY_ONCE,
    ;


}
