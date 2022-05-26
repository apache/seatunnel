/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.task;

import org.apache.seatunnel.engine.api.transform.AbstractTransformation;
import org.apache.seatunnel.engine.api.type.Row;
import org.apache.seatunnel.engine.utils.Collector;

import java.util.List;

public class BatchTaskSourceCollector implements Collector<Row> {
    private List<AbstractTransformation> transformations;
    private Channel channel;

    public BatchTaskSourceCollector(List<AbstractTransformation> transformations, Channel channel) {
        this.transformations = transformations;
        this.channel = channel;
    }

    @Override
    public void collect(Row record) {
        if (transformations != null) {
            transformations.forEach(transformation -> transformation.evaluate(record));
        }
        channel.push(record);
    }
}
