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

package org.apache.seatunnel.connectors.seatunnel.starrocks.serialize;

import org.apache.seatunnel.api.table.type.RowKind;

/**
 * Reference
 * https://github.com/StarRocks/starrocks/blob/main/docs/loading/Load_to_Primary_Key_tables.md#upsert-and-delete
 */
public enum StarRocksSinkOP {
    UPSERT,
    DELETE;

    public static final String COLUMN_KEY = "__op";

    static StarRocksSinkOP parse(RowKind kind) {
        switch (kind) {
            case INSERT:
            case UPDATE_AFTER:
                return UPSERT;
            case DELETE:
            case UPDATE_BEFORE:
                return DELETE;
            default:
                throw new RuntimeException("Unsupported row kind.");
        }
    }
}
