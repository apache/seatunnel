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

package org.apache.seatunnel.connector.selectdb.sink.writer;

import org.apache.seatunnel.connector.selectdb.exception.SelectDBConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.seatunnel.connector.selectdb.exception.SelectDBConnectorErrorCode.BUFFER_STOP_FAILED;

@Slf4j
public class RecordBuffer {
    BlockingQueue<ByteBuffer> writeQueue;
    BlockingQueue<ByteBuffer> readQueue;
    int bufferCapacity;
    int queueSize;
    ByteBuffer currentWriteBuffer;
    ByteBuffer currentReadBuffer;

    public RecordBuffer(int capacity, int queueSize) {
        log.info("init RecordBuffer capacity {}, count {}", capacity, queueSize);
        checkState(capacity > 0);
        checkState(queueSize > 1);
        this.writeQueue = new ArrayBlockingQueue<>(queueSize);
        for (int index = 0; index < queueSize; index++) {
            this.writeQueue.add(ByteBuffer.allocate(capacity));
        }
        readQueue = new LinkedBlockingDeque<>();
        this.bufferCapacity = capacity;
        this.queueSize = queueSize;
    }

    public void startBufferData() {
        log.info(
                "start buffer data, read queue size {}, write queue size {}",
                readQueue.size(),
                writeQueue.size());
        checkState(readQueue.size() == 0);
        checkState(writeQueue.size() == queueSize);
        for (ByteBuffer byteBuffer : writeQueue) {
            checkState(byteBuffer.position() == 0);
            checkState(byteBuffer.remaining() == bufferCapacity);
        }
    }

    public void stopBufferData() throws IOException {
        try {
            // add Empty buffer as finish flag.
            boolean isEmpty = false;
            if (currentWriteBuffer != null) {
                currentWriteBuffer.flip();
                // check if the current write buffer is empty.
                isEmpty = currentWriteBuffer.limit() == 0;
                readQueue.put(currentWriteBuffer);
                currentWriteBuffer = null;
            }
            if (!isEmpty) {
                ByteBuffer byteBuffer = writeQueue.take();
                byteBuffer.flip();
                checkState(byteBuffer.limit() == 0);
                readQueue.put(byteBuffer);
            }
        } catch (Exception e) {
            throw new SelectDBConnectorException(BUFFER_STOP_FAILED, e);
        }
    }

    public void write(byte[] buf) throws InterruptedException {
        int wPos = 0;
        do {
            if (currentWriteBuffer == null) {
                currentWriteBuffer = writeQueue.take();
            }
            int available = currentWriteBuffer.remaining();
            int nWrite = Math.min(available, buf.length - wPos);
            currentWriteBuffer.put(buf, wPos, nWrite);
            wPos += nWrite;
            if (currentWriteBuffer.remaining() == 0) {
                currentWriteBuffer.flip();
                readQueue.put(currentWriteBuffer);
                currentWriteBuffer = null;
            }
        } while (wPos != buf.length);
    }

    public int read(byte[] buf) throws InterruptedException {
        if (currentReadBuffer == null) {
            currentReadBuffer = readQueue.take();
        }
        // add empty buffer as end flag
        if (currentReadBuffer.limit() == 0) {
            recycleBuffer(currentReadBuffer);
            currentReadBuffer = null;
            checkState(readQueue.size() == 0);
            return -1;
        }
        int available = currentReadBuffer.remaining();
        int nRead = Math.min(available, buf.length);
        currentReadBuffer.get(buf, 0, nRead);
        if (currentReadBuffer.remaining() == 0) {
            recycleBuffer(currentReadBuffer);
            currentReadBuffer = null;
        }
        return nRead;
    }

    private void recycleBuffer(ByteBuffer buffer) throws InterruptedException {
        buffer.clear();
        writeQueue.put(buffer);
    }

    public int getWriteQueueSize() {
        return writeQueue.size();
    }

    public int getReadQueueSize() {
        return readQueue.size();
    }
}
