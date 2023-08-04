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

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.RESUME_TOKEN_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.TIMESTAMP_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.maximumBsonTimestamp;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.ResumeToken.decodeTimestamp;

public class ChangeStreamOffset extends Offset {

    private static final long serialVersionUID = 1L;

    public static final ChangeStreamOffset NO_STOPPING_OFFSET =
            new ChangeStreamOffset(maximumBsonTimestamp());

    public ChangeStreamOffset(Map<String, String> offset) {
        this.offset = offset;
    }

    public ChangeStreamOffset(BsonDocument resumeToken) {
        Objects.requireNonNull(resumeToken);
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(TIMESTAMP_FIELD, String.valueOf(decodeTimestamp(resumeToken).getValue()));
        offsetMap.put(RESUME_TOKEN_FIELD, resumeToken.toJson());
        this.offset = offsetMap;
    }

    public ChangeStreamOffset(BsonTimestamp timestamp) {
        Objects.requireNonNull(timestamp);
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(TIMESTAMP_FIELD, String.valueOf(timestamp.getValue()));
        offsetMap.put(RESUME_TOKEN_FIELD, null);
        this.offset = offsetMap;
    }

    @Nullable public BsonDocument getResumeToken() {
        String resumeTokenJson = offset.get(RESUME_TOKEN_FIELD);
        return Optional.ofNullable(resumeTokenJson).map(BsonDocument::parse).orElse(null);
    }

    public BsonTimestamp getTimestamp() {
        long timestamp = Long.parseLong(offset.get(TIMESTAMP_FIELD));
        return new BsonTimestamp(timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ChangeStreamOffset)) {
            return false;
        }
        ChangeStreamOffset that = (ChangeStreamOffset) o;
        return offset.equals(that.offset);
    }

    @Override
    public int compareTo(Offset offset) {
        if (offset == null) {
            return -1;
        }
        ChangeStreamOffset that = (ChangeStreamOffset) offset;
        return this.getTimestamp().compareTo(that.getTimestamp());
    }
}
