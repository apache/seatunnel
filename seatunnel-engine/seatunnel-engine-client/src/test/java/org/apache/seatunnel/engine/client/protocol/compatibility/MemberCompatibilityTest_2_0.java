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

package org.apache.seatunnel.engine.client.protocol.compatibility;

import com.hazelcast.client.HazelcastClientUtil;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageReader;
import com.hazelcast.client.impl.protocol.codec.*;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.client.impl.protocol.ClientMessage.IS_FINAL_FLAG;
import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberCompatibilityTest_2_0 {
    private final List<ClientMessage> clientMessages = new ArrayList<>();

    @Before
    public void setUp() throws IOException {
        File file = new File(getClass().getResource("/2.0.protocol.compatibility.binary").getFile());
        InputStream inputStream = new FileInputStream(file);
        byte[] data = new byte[(int) file.length()];
        inputStream.read(data);
        ByteBuffer buffer = ByteBuffer.wrap(data);
        ClientMessageReader reader = new ClientMessageReader(0);
        while (reader.readFrom(buffer, true)) {
            clientMessages.add(reader.getClientMessage());
            reader.reset();
        }
    }

    @Test
    public void test_EnginePrintMessageCodec_decodeRequest() {
        int fileClientMessageIndex = 0;
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        assertTrue(isEqual(aString, EnginePrintMessageCodec.decodeRequest(fromFile)));
    }

    @Test
    public void test_EnginePrintMessageCodec_encodeResponse() {
        int fileClientMessageIndex = 1;
        ClientMessage encoded = EnginePrintMessageCodec.encodeResponse(aString);
        ClientMessage fromFile = clientMessages.get(fileClientMessageIndex);
        compareClientMessages(fromFile, encoded);
    }

    private void compareClientMessages(ClientMessage binaryMessage, ClientMessage encodedMessage) {
        ClientMessage.Frame binaryFrame, encodedFrame;

        ClientMessage.ForwardFrameIterator binaryFrameIterator = binaryMessage.frameIterator();
        ClientMessage.ForwardFrameIterator encodedFrameIterator = encodedMessage.frameIterator();
        assertTrue("Client message that is read from the binary file does not have any frames", binaryFrameIterator.hasNext());

        while (binaryFrameIterator.hasNext()) {
            binaryFrame = binaryFrameIterator.next();
            encodedFrame = encodedFrameIterator.next();
            assertNotNull("Encoded client message has less frames.", encodedFrame);

            if (binaryFrame.isEndFrame() && !encodedFrame.isEndFrame()) {
                if (encodedFrame.isBeginFrame()) {
                    HazelcastClientUtil.fastForwardToEndFrame(encodedFrameIterator);
                }
                encodedFrame = HazelcastClientUtil.fastForwardToEndFrame(encodedFrameIterator);
            }

            boolean isFinal = binaryFrameIterator.peekNext() == null;
            int flags = isFinal ? encodedFrame.flags | IS_FINAL_FLAG : encodedFrame.flags;
            assertArrayEquals("Frames have different contents", binaryFrame.content, Arrays.copyOf(encodedFrame.content, binaryFrame.content.length));
            assertEquals("Frames have different flags", binaryFrame.flags, flags);
        }
    }
}