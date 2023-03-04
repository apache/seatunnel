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

package org.apache.seatunnel.engine.core.dag.actions;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Tolerate;

import java.util.concurrent.TimeUnit;

@Getter
@Setter
@ToString
@Builder(toBuilder = true)
public class ShuffleConfig implements Config {
    public static final int DEFAULT_BATCH_SIZE = 1024;
    public static final long DEFAULT_BATCH_FLUSH_INTERVAL = TimeUnit.SECONDS.toMillis(3);

    @Builder.Default private int batchSize = DEFAULT_BATCH_SIZE;
    @Builder.Default private long batchFlushInterval = DEFAULT_BATCH_FLUSH_INTERVAL;
    private ShuffleStrategy shuffleStrategy;

    @Tolerate
    public ShuffleConfig() {}
}
