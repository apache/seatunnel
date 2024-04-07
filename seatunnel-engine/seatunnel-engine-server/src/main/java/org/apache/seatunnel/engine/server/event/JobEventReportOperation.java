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

package org.apache.seatunnel.engine.server.event;

import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.event.EventProcessor;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
public class JobEventReportOperation extends Operation implements IdentifiedDataSerializable {

    private List<Event> events;

    @Override
    public void run() throws Exception {
        SeaTunnelServer server = getService();
        EventProcessor processor = server.getCoordinatorService().getEventProcessor();
        for (Event event : events) {
            processor.process(event);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                ObjectOutputStream objectOut = new ObjectOutputStream(byteOut)) {
            objectOut.writeObject(events);
            objectOut.flush();
            out.writeByteArray(byteOut.toByteArray());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        try (ByteArrayInputStream byteIn = new ByteArrayInputStream(in.readByteArray());
                ObjectInputStream objectIn = new ObjectInputStream(byteIn)) {
            events = (List<Event>) objectIn.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.REPORT_JOB_EVENT;
    }
}
