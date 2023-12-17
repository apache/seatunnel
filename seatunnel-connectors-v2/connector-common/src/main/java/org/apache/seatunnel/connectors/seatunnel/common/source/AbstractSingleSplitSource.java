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

package org.apache.seatunnel.connectors.seatunnel.common.source;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

public abstract class AbstractSingleSplitSource<T>
        implements SeaTunnelSource<T, SingleSplit, SingleSplitEnumeratorState> {

    @Override
    public final AbstractSingleSplitReader<T> createReader(SourceReader.Context readerContext)
            throws Exception {
        checkArgument(
                readerContext.getIndexOfSubtask() == 0,
                "A single split source allows only one single reader to be created. Please make sure source parallelism = 1");
        return createReader(new SingleSplitReaderContext(readerContext));
    }

    public abstract AbstractSingleSplitReader<T> createReader(
            SingleSplitReaderContext readerContext) throws Exception;

    @Override
    public final SourceSplitEnumerator<SingleSplit, SingleSplitEnumeratorState> createEnumerator(
            SourceSplitEnumerator.Context<SingleSplit> enumeratorContext) throws Exception {
        return new SingleSplitEnumerator(enumeratorContext);
    }

    @Override
    public final SourceSplitEnumerator<SingleSplit, SingleSplitEnumeratorState> restoreEnumerator(
            SourceSplitEnumerator.Context<SingleSplit> enumeratorContext,
            SingleSplitEnumeratorState checkpointState)
            throws Exception {
        return createEnumerator(enumeratorContext);
    }

    @Override
    public final Serializer<SingleSplit> getSplitSerializer() {
        return new DefaultSerializer<>();
    }
}
