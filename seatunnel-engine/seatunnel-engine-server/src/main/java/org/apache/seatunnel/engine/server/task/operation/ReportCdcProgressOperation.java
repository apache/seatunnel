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

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.observability.cdc.CdcEnumeratorProgressEnvelope;
import org.apache.seatunnel.engine.server.observability.cdc.CdcReaderProgressEnvelope;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Sends the latest worker-local CDC progress reports to the master. */
public class ReportCdcProgressOperation extends TracingOperation
        implements IdentifiedDataSerializable {

    private List<CdcReaderProgressEnvelope> readerReports;
    private List<CdcEnumeratorProgressEnvelope> enumeratorReports;

    public ReportCdcProgressOperation() {}

    public ReportCdcProgressOperation(
            List<CdcReaderProgressEnvelope> readerReports,
            List<CdcEnumeratorProgressEnvelope> enumeratorReports) {
        this.readerReports = readerReports;
        this.enumeratorReports = enumeratorReports;
    }

    @Override
    public void runInternal() throws Exception {
        SeaTunnelServer seaTunnelServer = getService();
        seaTunnelServer.getCdcProgressService().updateReaderReports(readerReports);
        seaTunnelServer.getCdcProgressService().updateEnumeratorReports(enumeratorReports);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(readerReports.size());
        for (CdcReaderProgressEnvelope report : readerReports) {
            CdcProgressReportSerializer.writeReaderEnvelope(out, report);
        }
        out.writeInt(enumeratorReports.size());
        for (CdcEnumeratorProgressEnvelope report : enumeratorReports) {
            CdcProgressReportSerializer.writeEnumeratorEnvelope(out, report);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int readerReportCount = CdcProgressReportSerializer.readSize(in, "reader report");
        readerReports = new ArrayList<>(readerReportCount);
        for (int i = 0; i < readerReportCount; i++) {
            readerReports.add(CdcProgressReportSerializer.readReaderEnvelope(in));
        }
        int enumeratorReportCount = CdcProgressReportSerializer.readSize(in, "enumerator report");
        enumeratorReports = new ArrayList<>(enumeratorReportCount);
        for (int i = 0; i < enumeratorReportCount; i++) {
            enumeratorReports.add(CdcProgressReportSerializer.readEnumeratorEnvelope(in));
        }
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.REPORT_CDC_PROGRESS_OPERATION;
    }
}
