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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.split;

import org.apache.seatunnel.api.source.SourceSplit;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Set;

/** RabbitMQ source split representing a specific queue. */
@Getter
@EqualsAndHashCode
@Setter
@AllArgsConstructor
public final class RabbitmqSplit implements SourceSplit {
    private static final long serialVersionUID = -678845022239224163L;

    /** Unique identifier for the split. */
    private String splitId;

    /** List of delivery tags associated with this split. */
    private List<Long> deliveryTags;

    /** Set of correlation IDs associated with this split. */
    private Set<String> correlationIds;

    /**
     * Constructor used during the split discovery phase.
     *
     * @param splitId A unique identifier for this split (synchronized with queueName for
     *     multi-table).
     */
    public RabbitmqSplit(final String splitId) {
        this.splitId = splitId;
        this.deliveryTags = null;
        this.correlationIds = null;
    }

    @Override
    public String splitId() {
        return splitId;
    }
}
