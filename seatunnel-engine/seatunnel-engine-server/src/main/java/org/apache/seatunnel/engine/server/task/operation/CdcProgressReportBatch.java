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

package org.apache.seatunnel.engine.server.task.operation;

import org.apache.seatunnel.engine.server.observability.cdc.CdcProgressEnvelope;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Explicitly serialized response containing connector-owned CDC progress reports. */
public class CdcProgressReportBatch implements IdentifiedDataSerializable {

    private List<CdcProgressEnvelope<?>> reports;

    public CdcProgressReportBatch() {}

    public CdcProgressReportBatch(List<? extends CdcProgressEnvelope<?>> reports) {
        this.reports = new ArrayList<>(reports);
    }

    public List<CdcProgressEnvelope<?>> getReports() {
        return Collections.unmodifiableList(reports);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(reports.size());
        for (CdcProgressEnvelope<?> report : reports) {
            CdcProgressReportSerializer.writeEnvelope(out, report);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int reportCount = CdcProgressReportSerializer.readSize(in, "report");
        reports = new ArrayList<>(reportCount);
        for (int i = 0; i < reportCount; i++) {
            reports.add(CdcProgressReportSerializer.readEnvelope(in));
        }
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.CDC_PROGRESS_REPORT_BATCH;
    }
}
