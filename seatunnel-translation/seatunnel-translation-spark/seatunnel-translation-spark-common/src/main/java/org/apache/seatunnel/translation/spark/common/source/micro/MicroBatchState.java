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

package org.apache.seatunnel.translation.spark.common.source.micro;

import org.apache.seatunnel.common.utils.SerializationUtils;

import org.apache.spark.sql.sources.v2.reader.streaming.Offset;

import java.io.Serializable;

public class MicroBatchState extends Offset implements Serializable {

    private final Integer checkpointId;

    public MicroBatchState(Integer checkpointId) {
        this.checkpointId = checkpointId;
    }

    @Override
    public String json() {
        return SerializationUtils.objectToString(this);
    }

    public Integer getCheckpointId() {
        return checkpointId;
    }
}
