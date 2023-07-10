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

import io.debezium.relational.TableId;
import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.regex.Pattern;

@AllArgsConstructor
@Getter
public class ChangeStreamDescriptor implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String database;
    private final String collection;
    private final Pattern databaseRegex;
    private final Pattern namespaceRegex;

    @Nonnull
    public static ChangeStreamDescriptor collection(@Nonnull TableId collectionId) {
        return collection(collectionId.catalog(), collectionId.table());
    }

    @Nonnull
    public static ChangeStreamDescriptor collection(String database, String collection) {
        return new ChangeStreamDescriptor(database, collection, null, null);
    }

    @Nonnull
    public static ChangeStreamDescriptor database(String database) {
        return new ChangeStreamDescriptor(database, null, null, null);
    }

    @Nonnull
    public static ChangeStreamDescriptor database(String database, Pattern namespaceRegex) {
        return new ChangeStreamDescriptor(database, null, null, namespaceRegex);
    }

    @Nonnull
    public static ChangeStreamDescriptor deployment(Pattern databaseRegex) {
        return new ChangeStreamDescriptor(null, null, databaseRegex, null);
    }

    @Nonnull
    public static ChangeStreamDescriptor deployment(Pattern databaseRegex, Pattern namespaceRegex) {
        return new ChangeStreamDescriptor(null, null, databaseRegex, namespaceRegex);
    }

    @Nonnull
    public static ChangeStreamDescriptor deployment() {
        return new ChangeStreamDescriptor(null, null, null, null);
    }
}
