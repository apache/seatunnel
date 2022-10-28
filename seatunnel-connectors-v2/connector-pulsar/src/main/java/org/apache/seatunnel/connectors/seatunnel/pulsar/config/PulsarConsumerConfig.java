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

package org.apache.seatunnel.connectors.seatunnel.pulsar.config;

// TODO: more field

import org.apache.pulsar.shade.com.google.common.base.Preconditions;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

public class PulsarConsumerConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String subscriptionName;

    private PulsarConsumerConfig(String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }

    public String getSubscriptionName() {
        return subscriptionName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String subscriptionName;

        private Builder() {
        }

        public Builder subscriptionName(String subscriptionName) {
            this.subscriptionName = subscriptionName;
            return this;
        }

        public PulsarConsumerConfig build() {
            Preconditions.checkArgument(StringUtils.isNotBlank(subscriptionName), "Pulsar subscription name is required.");
            return new PulsarConsumerConfig(subscriptionName);
        }
    }
}
