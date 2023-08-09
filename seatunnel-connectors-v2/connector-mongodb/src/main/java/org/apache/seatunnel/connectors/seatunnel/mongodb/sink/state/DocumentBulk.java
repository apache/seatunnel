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

package org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state;

import org.bson.BsonDocument;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * DocumentBulk is buffered {@link BsonDocument} in memory, which would be written to MongoDB in a
 * single transaction. Due to execution efficiency, each DocumentBulk maybe be limited to a maximum
 * size, typically 1,000 documents. But for the transactional mode, the maximum size should not be
 * respected because all that data must be written in one transaction.
 */
@ToString
@EqualsAndHashCode
public class DocumentBulk implements Serializable {

    public static final int BUFFER_SIZE = 1024;

    private final List<BsonDocument> bufferedDocuments;

    public DocumentBulk() {
        bufferedDocuments = new ArrayList<>(BUFFER_SIZE);
    }

    public void add(BsonDocument document) {
        if (bufferedDocuments.size() == BUFFER_SIZE) {
            throw new IllegalStateException("DocumentBulk is already full");
        }
        bufferedDocuments.add(document);
    }

    public int size() {
        return bufferedDocuments.size();
    }

    public List<BsonDocument> getDocuments() {
        return bufferedDocuments;
    }
}
