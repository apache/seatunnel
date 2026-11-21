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

package org.apache.seatunnel.connectors.seatunnel.iceberg.utils;

import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.DelegatingInputStream;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HadoopStreamFileIOTest {

    @Test
    void wrapsLegacyHadoopFileWithoutExposingItsStreamType() throws IOException {
        HadoopInputFile file = mock(HadoopInputFile.class);
        SeekableInputStream delegate = mock(SeekableInputStream.class);
        when(file.getLength()).thenReturn(3L);
        when(file.location()).thenReturn("file:///legacy.parquet");
        when(file.exists()).thenReturn(true);
        when(file.newStream()).thenReturn(delegate);

        InputFile wrapped = HadoopStreamFileIO.wrap(file, false);
        assertNotSame(file, wrapped);
        assertEquals(3L, wrapped.getLength());
        assertEquals("file:///legacy.parquet", wrapped.location());
        assertTrue(wrapped.exists());

        byte[] bytes = new byte[2];
        when(delegate.getPos()).thenReturn(1L);
        when(delegate.read()).thenReturn(7);
        when(delegate.read(bytes, 0, bytes.length)).thenReturn(2);
        when(delegate.skip(1)).thenReturn(1L);
        when(delegate.available()).thenReturn(2);

        try (SeekableInputStream stream = wrapped.newStream()) {
            assertFalse(stream instanceof DelegatingInputStream);
            assertEquals(1L, stream.getPos());
            stream.seek(2L);
            assertEquals(7, stream.read());
            assertEquals(2, stream.read(bytes, 0, bytes.length));
            assertEquals(1L, stream.skip(1));
            assertEquals(2, stream.available());
        }

        verify(delegate).seek(2L);
        verify(delegate).close();
    }

    @Test
    void keepsSupportedHadoopAndOtherInputFilesUnchanged() {
        HadoopInputFile hadoopFile = mock(HadoopInputFile.class);
        InputFile otherFile = mock(InputFile.class);

        assertSame(hadoopFile, HadoopStreamFileIO.wrap(hadoopFile, true));
        assertSame(otherFile, HadoopStreamFileIO.wrap(otherFile, false));
    }

    @Test
    void propagatesLegacyStreamFailures() throws IOException {
        HadoopInputFile file = mock(HadoopInputFile.class);
        SeekableInputStream delegate = mock(SeekableInputStream.class);
        when(file.newStream()).thenReturn(delegate);
        IOException failure = new IOException("seek failed");
        doThrow(failure).when(delegate).seek(1L);

        try (SeekableInputStream stream = HadoopStreamFileIO.wrap(file, false).newStream()) {
            assertSame(failure, assertThrows(IOException.class, () -> stream.seek(1L)));
        }
        verify(delegate).close();
    }
}
