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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.progress;

import org.apache.seatunnel.api.cdc.CdcProgressPosition;
import org.apache.seatunnel.api.cdc.CdcProgressSnapshot;
import org.apache.seatunnel.connectors.cdc.base.source.progress.CdcProgressSnapshots;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;

/** MySQL CDC progress helpers. */
public final class MySqlCdcProgress {

    public static final String CONNECTOR_TYPE = "MySQL CDC";

    private MySqlCdcProgress() {}

    public static CdcProgressSnapshot forIncrementalSplit(
            IncrementalSplit split, long lastProgressTime) {
        return CdcProgressSnapshots.forIncrementalSplit(CONNECTOR_TYPE, split, lastProgressTime);
    }

    public static CdcProgressPosition toPosition(BinlogOffset offset) {
        return CdcProgressSnapshots.toPosition(offset);
    }
}
