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

package org.apache.seatunnel.connectors.seatunnel.common.source.reader;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;

@Getter
@SuppressWarnings("MagicNumber")
public class SourceReaderOptions {
    public static final Option<Long> SOURCE_READER_CLOSE_TIMEOUT =
            Options.key("source.reader.close.timeout")
                    .longType()
                    .defaultValue(60000L)
                    .withDescription("The timeout when closing the source reader");

    public static final Option<Integer> ELEMENT_QUEUE_CAPACITY =
            Options.key("source.reader.element.queue.capacity")
                    .intType()
                    .defaultValue(2)
                    .withDescription("The capacity of the element queue in the source reader.");

    public final long sourceReaderCloseTimeout;
    public final int elementQueueCapacity;

    public SourceReaderOptions(Config config) {
        this(ReadonlyConfig.fromConfig(config));
    }

    public SourceReaderOptions(ReadonlyConfig config) {
        this.sourceReaderCloseTimeout = config.get(SOURCE_READER_CLOSE_TIMEOUT);
        this.elementQueueCapacity = config.get(ELEMENT_QUEUE_CAPACITY);
    }
}
