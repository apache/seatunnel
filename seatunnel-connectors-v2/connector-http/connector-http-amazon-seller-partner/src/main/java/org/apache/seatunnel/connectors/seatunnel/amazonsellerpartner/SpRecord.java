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

package org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

/**
 * One decoded record with its business identity and optional parent.
 *
 * <p>Report rows without a provider primary key use a document-scoped identity assigned by the
 * report reader.
 */
final class SpRecord {
    final String id;
    final String parent;
    final JsonNode payload;

    SpRecord(String id, String parent, JsonNode payload) {
        this.id = id;
        this.parent = parent;
        this.payload = payload;
    }
}
