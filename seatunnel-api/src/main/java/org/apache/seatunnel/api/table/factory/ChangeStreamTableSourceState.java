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

import org.apache.seatunnel.api.source.SourceSplit;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * The state of the enumerator and splits of the enumerator, which is used to resume the enumerator
 * and reader.
 *
 * @param <StateT>
 * @param <SplitT>
 */
@Data
@AllArgsConstructor
public class ChangeStreamTableSourceState<StateT extends Serializable, SplitT extends SourceSplit> {
    // The state of the enumerator, which is used to resume the enumerator.
    private StateT enumeratorState;

    // The splits of the enumerator, which is used to resume the reader.
    public List<List<SplitT>> splits;
}
