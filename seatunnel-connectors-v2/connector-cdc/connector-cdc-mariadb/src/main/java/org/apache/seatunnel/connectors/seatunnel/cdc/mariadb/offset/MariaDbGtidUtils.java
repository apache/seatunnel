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

package org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.offset;

import java.util.HashMap;
import java.util.Map;

/** Utils for handling MariaDB GTIDs. */
public class MariaDbGtidUtils {

    /**
     * Corrects the restored GTID set using the GTID set fetched from the server. Ensures sequence
     * numbers do not exceed the server's current GTID sequence for matching domains.
     */
    public static MariaDbGtidSet fixRestoredGtidSet(
            MariaDbGtidSet serverGtidSet, MariaDbGtidSet restoredGtidSet) {
        if (serverGtidSet == null || serverGtidSet.isEmpty()) {
            return restoredGtidSet;
        }
        if (restoredGtidSet == null || restoredGtidSet.isEmpty()) {
            return serverGtidSet;
        }

        Map<Long, MariaDbGtidSet.MariaDbGtid> newSet = new HashMap<>(serverGtidSet.getGtids());
        for (MariaDbGtidSet.MariaDbGtid restoredGtid : restoredGtidSet.getGtids().values()) {
            MariaDbGtidSet.MariaDbGtid serverGtid = newSet.get(restoredGtid.getDomainId());
            if (serverGtid != null) {
                long adjustedSeq = Math.min(serverGtid.getSequence(), restoredGtid.getSequence());
                newSet.put(
                        restoredGtid.getDomainId(),
                        new MariaDbGtidSet.MariaDbGtid(
                                restoredGtid.getDomainId(),
                                restoredGtid.getServerId(),
                                adjustedSeq));
            } else {
                newSet.put(restoredGtid.getDomainId(), restoredGtid);
            }
        }
        return new MariaDbGtidSet(newSet);
    }

    /**
     * Merges one GTID set (toMerge) into another (base), keeping higher sequence numbers for
     * duplicate domains.
     */
    public static MariaDbGtidSet mergeGtidSetInto(MariaDbGtidSet base, MariaDbGtidSet toMerge) {
        if (base == null || base.isEmpty()) {
            return toMerge;
        }
        if (toMerge == null || toMerge.isEmpty()) {
            return base;
        }

        Map<Long, MariaDbGtidSet.MariaDbGtid> newSet = new HashMap<>(base.getGtids());
        for (MariaDbGtidSet.MariaDbGtid gtid : toMerge.getGtids().values()) {
            MariaDbGtidSet.MariaDbGtid existing = newSet.get(gtid.getDomainId());
            if (existing == null || gtid.getSequence() > existing.getSequence()) {
                newSet.put(gtid.getDomainId(), gtid);
            }
        }
        return new MariaDbGtidSet(newSet);
    }
}
