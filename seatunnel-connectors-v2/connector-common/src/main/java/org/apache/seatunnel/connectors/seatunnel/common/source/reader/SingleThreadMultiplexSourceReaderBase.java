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

import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

/**
 * A base for {@link SourceReader}s that read splits with one thread using one {@link SplitReader}.
 *
 * @param <E> The type of the records (the raw type that typically contains checkpointing information).
 *
 * @param <T> The final type of the records emitted by the source.
 *
 * @param <SplitT>
 *
 * @param <SplitStateT>
 *
 */
public abstract class SingleThreadMultiplexSourceReaderBase<E, T, SplitT extends SourceSplit, SplitStateT>
    extends SourceReaderBase<E, T, SplitT, SplitStateT> {

    public SingleThreadMultiplexSourceReaderBase(Supplier<SplitReader<E, SplitT>> splitReaderSupplier,
                                                 RecordEmitter<E, T, SplitStateT> recordEmitter,
                                                 SourceReaderOptions options,
                                                 SourceReader.Context context) {
        this(new ArrayBlockingQueue<>(options.getElementQueueCapacity()),
            splitReaderSupplier,
            recordEmitter,
            options,
            context);
    }

    public SingleThreadMultiplexSourceReaderBase(BlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
                                                 Supplier<SplitReader<E, SplitT>> splitReaderSupplier,
                                                 RecordEmitter<E, T, SplitStateT> recordEmitter,
                                                 SourceReaderOptions options,
                                                 SourceReader.Context context) {
        super(elementsQueue,
            new SingleThreadFetcherManager<>(elementsQueue, splitReaderSupplier),
            recordEmitter,
            options,
            context);
    }

    public SingleThreadMultiplexSourceReaderBase(BlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
                                                 SingleThreadFetcherManager<E, SplitT> splitFetcherManager,
                                                 RecordEmitter<E, T, SplitStateT> recordEmitter,
                                                 SourceReaderOptions options,
                                                 SourceReader.Context context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, options, context);
    }
}
