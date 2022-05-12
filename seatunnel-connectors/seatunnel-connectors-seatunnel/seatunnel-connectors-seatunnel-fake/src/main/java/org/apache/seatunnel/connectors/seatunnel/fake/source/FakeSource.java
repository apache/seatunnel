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

package org.apache.seatunnel.connectors.seatunnel.fake.source;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.fake.state.FakeState;

public class FakeSource implements SeaTunnelSource<FakeSourceEvent, FakeSourceSplit, FakeState> {

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<FakeSourceEvent, FakeSourceSplit> createReader(SourceReader.Context readerContext) {
        return new FakeSourceReader(readerContext);
    }

    @Override
    public Serializer<FakeSourceSplit> getSplitSerializer() {
        return new ObjectSerializer<>();
    }

    @Override
    public SourceSplitEnumerator<FakeSourceSplit, FakeState> createEnumerator(
        SourceSplitEnumerator.Context<FakeSourceSplit> enumeratorContext) {
        return new FakeSourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<FakeSourceSplit, FakeState> restoreEnumerator(
        SourceSplitEnumerator.Context<FakeSourceSplit> enumeratorContext, FakeState checkpointState) {
        // todo:
        return null;
    }

    @Override
    public Serializer<FakeState> getEnumeratorStateSerializer() {
        return new ObjectSerializer<>();
    }
}
