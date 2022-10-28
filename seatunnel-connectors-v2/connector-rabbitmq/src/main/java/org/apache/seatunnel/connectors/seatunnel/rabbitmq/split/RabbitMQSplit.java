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

import com.sun.istack.internal.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import org.apache.seatunnel.api.source.SourceSplit;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
public class RabbitMQSplit implements SourceSplit {
    @Nullable
    private List<Long> deliveryTags;

    @Override
    public String splitId() {
        return "";
    }
    public RabbitMQSplit copy() {
        return new RabbitMQSplit(deliveryTags);
    }

}
