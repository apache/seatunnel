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

package org.apache.seatunnel.api.table.factory;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public interface ChangeStreamTableSourceFactory extends TableSourceFactory {

    /**
     * see {@link SeaTunnelSource#getSplitSerializer()}.
     *
     * @return
     * @param <SplitT>
     */
    default <SplitT extends SourceSplit> Serializer<SplitT> getSplitSerializer() {
        return new DefaultSerializer<>();
    }

    /**
     * see {@link SeaTunnelSource#getEnumeratorStateSerializer()}.
     *
     * @return
     * @param <StateT>
     */
    default <StateT extends Serializable> Serializer<StateT> getEnumeratorStateSerializer() {
        return new DefaultSerializer<>();
    }

    /**
     * Create a {@link ChangeStreamTableSourceState} from the given {@link
     * ChangeStreamTableSourceCheckpoint}. The default implementation uses the {@link
     * #getSplitSerializer()} and {@link #getEnumeratorStateSerializer()} to deserialize the splits
     * and enumerator state.
     *
     * <p>If the splits or enumerator state is null, the corresponding field in the returned state
     * will be null.
     *
     * @param checkpoint
     * @return
     * @param <StateT>
     * @param <SplitT>
     * @throws IOException
     */
    default <StateT extends Serializable, SplitT extends SourceSplit>
            ChangeStreamTableSourceState<StateT, SplitT> deserializeTableSourceState(
                    ChangeStreamTableSourceCheckpoint checkpoint) throws IOException {
        StateT enumeratorState = null;
        if (checkpoint.getEnumeratorState() != null) {
            Serializer<StateT> enumeratorStateSerializer = getEnumeratorStateSerializer();
            enumeratorState =
                    enumeratorStateSerializer.deserialize(checkpoint.getEnumeratorState());
        }

        List<List<SplitT>> deserializedSplits = new ArrayList<>();
        if (checkpoint.getSplits() != null && !checkpoint.getSplits().isEmpty()) {
            Serializer<SplitT> splitSerializer = getSplitSerializer();
            List<List<byte[]>> splits = checkpoint.getSplits();
            for (int i = 0; i < splits.size(); i++) {
                List<byte[]> subTaskSplits = splits.get(i);
                if (subTaskSplits == null || subTaskSplits.isEmpty()) {
                    deserializedSplits.add(Collections.emptyList());
                } else {
                    List<SplitT> deserializedSubTaskSplits = new ArrayList<>(subTaskSplits.size());
                    for (byte[] split : subTaskSplits) {
                        if (split != null) {
                            deserializedSubTaskSplits.add(splitSerializer.deserialize(split));
                        }
                    }
                    deserializedSplits.add(deserializedSubTaskSplits);
                }
            }
        }
        return new ChangeStreamTableSourceState<>(enumeratorState, deserializedSplits);
    }

    /**
     * Restore the source from the checkpoint state.
     *
     * @param context
     * @param state checkpoint state
     * @return
     * @param <T>
     * @param <SplitT>
     * @param <StateT>
     */
    <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> restoreSource(
                    TableSourceFactoryContext context,
                    ChangeStreamTableSourceState<StateT, SplitT> state);
}
