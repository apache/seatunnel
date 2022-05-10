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

package org.apache.seatunnel.api.source;

import org.apache.seatunnel.api.serialization.Serializer;

import java.io.Serializable;

/**
 * The interface for Source. It acts like a factory class that helps construct the {@link
 * SourceSplitEnumerator} and {@link SourceReader} and corresponding serializers.
 *
 * @param <T>      The type of records produced by the source.
 * @param <SplitT> The type of splits handled by the source.
 */
public interface Source<T, SplitT extends SourceSplit, StateT> extends Serializable {

    /**
     * Get the boundedness of this source.
     *
     * @return the boundedness of this source.
     */
    Boundedness getBoundedness();

    SourceReader<T, SplitT> createReader(SourceReader.Context readerContext) throws Exception;

    Serializer<SplitT> getSplitSerializer();

    SourceSplitEnumerator<SplitT, StateT> createEnumerator(SourceSplitEnumerator.Context<SplitT> enumeratorContext) throws Exception;

    SourceSplitEnumerator<SplitT, StateT> restoreEnumerator(SourceSplitEnumerator.Context<SplitT> enumeratorContext, StateT checkpointState) throws Exception;

    Serializer<StateT> getEnumeratorStateSerializer();

}
