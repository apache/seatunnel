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
import lombok.Getter;
import lombok.Setter;

import java.util.Collections;
import java.util.List;
import java.util.Set;

@Getter
@Setter
@AllArgsConstructor
public class RabbitmqSplit implements SourceSplit {
    private static final long serialVersionUID = -678845022239224163L;
    private String splitId;
    private final String queueName;
    private List<Long> deliveryTags;
    private Set<String> correlationIds;

    public RabbitmqSplit(String splitId, String queueName) {
        this.splitId = splitId;
        this.queueName = queueName;
        this.deliveryTags = Collections.emptyList();
        this.correlationIds = Collections.emptySet();
    }

    public RabbitmqSplit(List<Long> deliveryTags, Set<String> correlationIds) {
        this.splitId = "default-split";
        this.queueName = null;
        this.deliveryTags = deliveryTags;
        this.correlationIds = correlationIds;
    }

    @Override
    public String splitId() {
        return queueName;
    }
}
