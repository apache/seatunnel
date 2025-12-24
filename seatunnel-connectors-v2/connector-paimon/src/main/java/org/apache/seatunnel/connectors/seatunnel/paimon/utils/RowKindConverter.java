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

package org.apache.seatunnel.connectors.seatunnel.paimon.utils;

import org.apache.seatunnel.api.table.type.RowKind;

public class RowKindConverter {

    /**
     * Convert SeaTunnel RowKind {@link RowKind} to Paimon RowKind {@link
     * org.apache.paimon.types.RowKind}
     *
     * @param seaTunnelRowKind The kind of change that a row describes in a changelog.
     * @return
     */
    public static org.apache.paimon.types.RowKind convertSeaTunnelRowKind2PaimonRowKind(
            RowKind seaTunnelRowKind) {
        switch (seaTunnelRowKind) {
            case DELETE:
                return org.apache.paimon.types.RowKind.DELETE;
            case UPDATE_AFTER:
                return org.apache.paimon.types.RowKind.UPDATE_AFTER;
            case UPDATE_BEFORE:
                return org.apache.paimon.types.RowKind.UPDATE_BEFORE;
            case INSERT:
                return org.apache.paimon.types.RowKind.INSERT;
            default:
                return null;
        }
    }

    /**
     * Convert Paimon RowKind {@link org.apache.paimon.types.RowKind} to SeaTunnel RowKind {@link
     * RowKind}
     *
     * @param paimonRowKind
     * @return
     */
    public static RowKind convertPaimonRowKind2SeatunnelRowkind(
            org.apache.paimon.types.RowKind paimonRowKind) {
        switch (paimonRowKind) {
            case DELETE:
                return RowKind.DELETE;
            case UPDATE_AFTER:
                return RowKind.UPDATE_AFTER;
            case UPDATE_BEFORE:
                return RowKind.UPDATE_BEFORE;
            case INSERT:
                return RowKind.INSERT;
            default:
                return null;
        }
    }
}
