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

package org.apache.seatunnel.connectors.selectdb.sink.writer;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;

@Slf4j
public class RecordBuffer {
    private String fileName;
    private StringJoiner buffer;
    private String lineDelimiter;
    private int numOfRecords = 0;
    private long bufferSizeBytes = 0;

    public RecordBuffer() {}

    public RecordBuffer(String lineDelimiter) {
        super();
        this.lineDelimiter = lineDelimiter;
        this.buffer = new StringJoiner(lineDelimiter);
    }

    public void insert(String record) {
        this.buffer.add(record);
        setNumOfRecords(getNumOfRecords() + 1);
        setBufferSizeBytes(getBufferSizeBytes() + record.getBytes(StandardCharsets.UTF_8).length);
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public boolean isEmpty() {
        return numOfRecords == 0;
    }

    public String getData() {
        String result = buffer.toString();
        log.debug("flush buffer: {} records, {} bytes", getNumOfRecords(), getBufferSizeBytes());
        return result;
    }

    public int getNumOfRecords() {
        return numOfRecords;
    }

    public long getBufferSizeBytes() {
        return bufferSizeBytes;
    }

    public void setNumOfRecords(int numOfRecords) {
        this.numOfRecords = numOfRecords;
    }

    public void setBufferSizeBytes(long bufferSizeBytes) {
        this.bufferSizeBytes = bufferSizeBytes;
    }
}
