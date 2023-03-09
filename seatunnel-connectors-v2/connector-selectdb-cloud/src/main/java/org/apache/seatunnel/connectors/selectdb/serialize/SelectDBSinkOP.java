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

package org.apache.seatunnel.connectors.selectdb.serialize;

import org.apache.seatunnel.api.table.type.RowKind;

public class SelectDBSinkOP {
    public static String parseDeleteSign(RowKind rowKind) {
        if (RowKind.INSERT.equals(rowKind) || RowKind.UPDATE_AFTER.equals(rowKind)) {
            return "0";
        } else if (RowKind.DELETE.equals(rowKind) || RowKind.UPDATE_BEFORE.equals(rowKind)) {
            return "1";
        } else {
            throw new IllegalArgumentException("Unrecognized row kind:" + rowKind.toString());
        }
    }
}
