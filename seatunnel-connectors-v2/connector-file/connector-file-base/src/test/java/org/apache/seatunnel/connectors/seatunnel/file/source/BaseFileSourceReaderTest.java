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

package org.apache.seatunnel.connectors.seatunnel.file.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;

class BaseFileSourceReaderTest {

    @Test
    void shouldPreserveNonMarkdownFailureContext() throws Exception {
        String source = "topic:v1";
        IOException failure = new IOException("read failed");
        ReadStrategy readStrategy = failingReadStrategy(source, failure);
        Collector<SeaTunnelRow> output = collector();
        BaseFileSourceReader reader =
                new BaseFileSourceReader(
                        readStrategy, Mockito.mock(SourceReader.Context.class), false);
        reader.addSplits(Collections.singletonList(new FileSourceSplit(source)));

        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class, () -> reader.pollNext(output));

        Assertions.assertEquals(source, exception.getParams().get("fileName"));
        Assertions.assertSame(failure, exception.getCause());
    }

    @Test
    void shouldSanitizeMarkdownFailureContextAndRetainStackTrace() throws Exception {
        String source = "https://user:secret@example.com/docs/a.md" + "?X-Amz-Signature=value#part";
        IOException failure = new IOException("read failed for " + source);
        failure.setStackTrace(
                new StackTraceElement[] {
                    new StackTraceElement("RemoteReader", "read", "RemoteReader.java", 42)
                });
        ReadStrategy readStrategy = failingReadStrategy(source, failure);
        Collector<SeaTunnelRow> output = collector();
        BaseFileSourceReader reader =
                new BaseFileSourceReader(
                        readStrategy, Mockito.mock(SourceReader.Context.class), true);
        reader.addSplits(Collections.singletonList(new FileSourceSplit(source)));

        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class, () -> reader.pollNext(output));

        Assertions.assertEquals(
                "https://example.com/docs/a.md", exception.getParams().get("fileName"));
        Assertions.assertNotNull(exception.getCause());
        Assertions.assertNotSame(failure, exception.getCause());
        Assertions.assertArrayEquals(failure.getStackTrace(), exception.getCause().getStackTrace());
        Assertions.assertTrue(
                exception.getCause().getMessage().contains(IOException.class.getName()));
        assertDoesNotExposeSensitiveSource(exception);
    }

    private static ReadStrategy failingReadStrategy(String source, IOException failure)
            throws Exception {
        ReadStrategy readStrategy = Mockito.mock(ReadStrategy.class);
        Mockito.doThrow(failure)
                .when(readStrategy)
                .read(Mockito.eq(source), Mockito.eq(""), Mockito.any());
        return readStrategy;
    }

    @SuppressWarnings("unchecked")
    private static Collector<SeaTunnelRow> collector() {
        Collector<SeaTunnelRow> output = Mockito.mock(Collector.class);
        Mockito.when(output.getCheckpointLock()).thenReturn(new Object());
        return output;
    }

    private static void assertDoesNotExposeSensitiveSource(Throwable throwable) {
        StringBuilder rendered = new StringBuilder();
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            rendered.append(current.getMessage()).append('\n');
        }
        String message = rendered.toString();
        Assertions.assertFalse(message.contains("user"));
        Assertions.assertFalse(message.contains("secret"));
        Assertions.assertFalse(message.contains("X-Amz-Signature"));
        Assertions.assertFalse(message.contains("value"));
        Assertions.assertFalse(message.contains("part"));
    }
}
