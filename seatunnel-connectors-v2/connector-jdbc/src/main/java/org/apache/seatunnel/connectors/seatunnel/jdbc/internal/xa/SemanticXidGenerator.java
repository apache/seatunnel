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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.sink.SinkWriter;

import javax.transaction.xa.Xid;

import java.security.SecureRandom;
import java.util.Arrays;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

/**
 * Generates {@link Xid} from:
 *
 * <ol>
 *   <li>To provide uniqueness over other jobs and apps, and other instances
 *   <li>of this job, gtrid consists of
 *   <li>job id (32 bytes)
 *   <li>subtask index (4 bytes)
 *   <li>checkpoint id (8 bytes)
 *   <li>bqual consists of 4 random bytes (generated using {@link SecureRandom})
 * </ol>
 *
 * <p>Each {@link SemanticXidGenerator} instance MUST be used for only one Sink (otherwise Xids will
 * collide).
 */
class SemanticXidGenerator implements XidGenerator {
    private static final long serialVersionUID = 1L;

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private static final int JOB_ID_BYTES = 32;
    private static final int FORMAT_ID = 201;

    private transient byte[] gtridBuffer;
    private transient byte[] bqualBuffer;

    @Override
    public void open() {
        // globalTransactionId = job id + task index + checkpoint id
        gtridBuffer = new byte[JOB_ID_BYTES + Integer.BYTES + Long.BYTES];
        // branchQualifier = random bytes
        bqualBuffer = getRandomBytes(Integer.BYTES);
    }

    @Override
    public Xid generateXid(JobContext context, SinkWriter.Context sinkContext, long checkpointId) {
        byte[] jobIdBytes = context.getJobId().getBytes();
        Arrays.fill(gtridBuffer, (byte) 0);
        checkArgument(jobIdBytes.length <= JOB_ID_BYTES);
        System.arraycopy(jobIdBytes, 0, gtridBuffer, 0, jobIdBytes.length);

        writeNumber(sinkContext.getIndexOfSubtask(), Integer.BYTES, gtridBuffer, JOB_ID_BYTES);
        writeNumber(checkpointId, Long.BYTES, gtridBuffer, JOB_ID_BYTES + Integer.BYTES);
        // relying on arrays copying inside XidImpl constructor
        return new XidImpl(FORMAT_ID, gtridBuffer, bqualBuffer);
    }

    @Override
    public boolean belongsToSubtask(Xid xid, JobContext context, SinkWriter.Context sinkContext) {
        if (xid.getFormatId() != FORMAT_ID) {
            return false;
        }
        int xidSubtaskIndex = readNumber(xid.getGlobalTransactionId(), JOB_ID_BYTES, Integer.BYTES);
        if (xidSubtaskIndex != sinkContext.getIndexOfSubtask()) {
            return false;
        }
        byte[] xidJobIdBytes = new byte[JOB_ID_BYTES];
        System.arraycopy(xid.getGlobalTransactionId(), 0, xidJobIdBytes, 0, JOB_ID_BYTES);

        byte[] jobIdBytes = new byte[JOB_ID_BYTES];
        byte[] bytes = context.getJobId().getBytes();
        System.arraycopy(bytes, 0, jobIdBytes, 0, bytes.length);

        return Arrays.equals(jobIdBytes, xidJobIdBytes);
    }

    private static int readNumber(byte[] bytes, int offset, int numBytes) {
        final int number = 0xff;
        int result = 0;
        for (int i = 0; i < numBytes; i++) {
            result |= (bytes[offset + i] & number) << Byte.SIZE * i;
        }
        return result;
    }

    private static void writeNumber(long number, int numBytes, byte[] dst, int dstOffset) {
        for (int i = dstOffset; i < dstOffset + numBytes; i++) {
            dst[i] = (byte) number;
            number >>>= Byte.SIZE;
        }
    }

    private byte[] getRandomBytes(int size) {
        byte[] bytes = new byte[size];
        SECURE_RANDOM.nextBytes(bytes);
        return bytes;
    }
}
