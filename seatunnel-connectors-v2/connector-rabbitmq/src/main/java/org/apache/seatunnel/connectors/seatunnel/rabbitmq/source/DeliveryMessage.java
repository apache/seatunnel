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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.source;

import com.rabbitmq.client.Delivery;
import lombok.Getter;

import java.io.Serializable;

/** RabbitMQ delivery message wrapper with split identifier. */
@Getter
public final class DeliveryMessage implements Serializable {

    /** Identifier of the split (usually the queue name). */
    private final String splitId;

    /** The actual RabbitMQ delivery data. */
    private final Delivery delivery;

    /**
     * Constructor for DeliveryMessage.
     *
     * @param splitId split identifier
     * @param delivery RabbitMQ delivery
     */
    public DeliveryMessage(final String splitId, final Delivery delivery) {
        this.splitId = splitId;
        this.delivery = delivery;
    }
}
