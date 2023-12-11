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

package org.apache.seatunnel.translation.flink.source;

import org.apache.flink.api.connector.source.SourceSplit;

/**
 * The {@link org.apache.seatunnel.api.source.SourceSplit} wrapper, used for proxy all seatunnel
 * user-defined source split in flink engine.
 *
 * @param <T> The generic type of source split
 */
public class SplitWrapper<T extends org.apache.seatunnel.api.source.SourceSplit>
        implements SourceSplit {

    private final T sourceSplit;

    public SplitWrapper(T sourceSplit) {
        this.sourceSplit = sourceSplit;
    }

    public T getSourceSplit() {
        return sourceSplit;
    }

    @Override
    public String splitId() {
        return sourceSplit.splitId();
    }
}
