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

package org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.scan;

public enum IcebergStreamScanStrategy {
    /**
     * Do a regular table scan then switch to the incremental mode.
     */
    TABLE_SCAN_THEN_INCREMENTAL,
    /**
     * Start incremental mode from the latest snapshot inclusive.
     */
    FROM_LATEST_SNAPSHOT,
    /**
     * Start incremental mode from the earliest snapshot inclusive.
     */
    FROM_EARLIEST_SNAPSHOT,
    /**
     * Start incremental mode from a snapshot with a specific id inclusive.
     */
    FROM_SNAPSHOT_ID,
    /**
     * Start incremental mode from a snapshot with a specific timestamp inclusive.
     */
    FROM_SNAPSHOT_TIMESTAMP
}
