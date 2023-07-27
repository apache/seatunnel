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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.exception.MongodbConnectorException;

import java.util.Map;

import static org.apache.seatunnel.common.exception.CommonErrorCode.UNSUPPORTED_OPERATION;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.bsonTimestampFromEpochMillis;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.currentBsonTimestamp;

public class ChangeStreamOffsetFactory extends OffsetFactory {

    @Override
    public Offset earliest() {
        return new ChangeStreamOffset(currentBsonTimestamp());
    }

    @Override
    public Offset neverStop() {
        return ChangeStreamOffset.NO_STOPPING_OFFSET;
    }

    @Override
    public Offset latest() {
        return new ChangeStreamOffset(currentBsonTimestamp());
    }

    @Override
    public Offset specific(Map<String, String> offset) {
        return new ChangeStreamOffset(offset);
    }

    @Override
    public Offset specific(String filename, Long position) {
        throw new MongodbConnectorException(
                UNSUPPORTED_OPERATION, "not supported create new Offset by filename and position.");
    }

    @Override
    public Offset timestamp(long timestamp) {
        return new ChangeStreamOffset(bsonTimestampFromEpochMillis(timestamp));
    }
}
