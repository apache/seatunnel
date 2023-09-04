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

package org.apache.seatunnel.connectors.seatunnel.mongodb.source.config;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.CURSOR_NO_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.FETCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.MAX_TIME_MIN;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

/** The configuration class for MongoDB source. */
@EqualsAndHashCode
@Getter
public class MongodbReadOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int fetchSize;

    private final boolean noCursorTimeout;

    private final long maxTimeMS;

    private MongodbReadOptions(int fetchSize, boolean noCursorTimeout, long maxTimeMS) {
        this.fetchSize = fetchSize;
        this.noCursorTimeout = noCursorTimeout;
        this.maxTimeMS = maxTimeMS;
    }

    public static MongoReadOptionsBuilder builder() {
        return new MongoReadOptionsBuilder();
    }

    /** Builder for {@link MongodbReadOptions}. */
    public static class MongoReadOptionsBuilder {

        private int fetchSize = FETCH_SIZE.defaultValue();

        private boolean noCursorTimeout = CURSOR_NO_TIMEOUT.defaultValue();

        private long maxTimeMin = MAX_TIME_MIN.defaultValue();

        private MongoReadOptionsBuilder() {}

        public MongoReadOptionsBuilder setFetchSize(int fetchSize) {
            checkArgument(fetchSize > 0, "The fetch size must be larger than 0.");
            this.fetchSize = fetchSize;
            return this;
        }

        public MongoReadOptionsBuilder setNoCursorTimeout(boolean noCursorTimeout) {
            this.noCursorTimeout = noCursorTimeout;
            return this;
        }

        public MongoReadOptionsBuilder setMaxTimeMS(long maxTimeMS) {
            this.maxTimeMin = maxTimeMS;
            return this;
        }

        public MongodbReadOptions build() {
            return new MongodbReadOptions(fetchSize, noCursorTimeout, maxTimeMin);
        }
    }
}
