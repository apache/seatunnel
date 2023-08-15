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

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import javax.transaction.xa.Xid;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

/**
 * A simple {@link Xid} implementation that stores branch and global transaction identifiers as byte
 * arrays.
 */
final class XidImpl implements Xid, Serializable {

    private static final long serialVersionUID = 1L;
    private static final char[] HEX_CHARS = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };

    private final int formatId;
    private final byte[] globalTransactionId;
    private final byte[] branchQualifier;

    public XidImpl(int formatId, byte[] globalTransactionId, byte[] branchQualifier) {
        checkArgument(globalTransactionId.length <= Xid.MAXGTRIDSIZE);
        checkArgument(branchQualifier.length <= Xid.MAXBQUALSIZE);
        this.formatId = formatId;
        this.globalTransactionId = Arrays.copyOf(globalTransactionId, globalTransactionId.length);
        this.branchQualifier = Arrays.copyOf(branchQualifier, branchQualifier.length);
    }

    @Override
    public int getFormatId() {
        return formatId;
    }

    @Override
    public byte[] getGlobalTransactionId() {
        return globalTransactionId;
    }

    @Override
    public byte[] getBranchQualifier() {
        return branchQualifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof XidImpl)) {
            return false;
        }
        XidImpl xid = (XidImpl) o;
        return formatId == xid.formatId
                && Arrays.equals(globalTransactionId, xid.globalTransactionId)
                && Arrays.equals(branchQualifier, xid.branchQualifier);
    }

    @Override
    public int hashCode() {
        final int number = 31;
        int result = Objects.hash(formatId);
        result = number * result + Arrays.hashCode(globalTransactionId);
        result = number * result + Arrays.hashCode(branchQualifier);
        return result;
    }

    @Override
    public String toString() {
        return formatId
                + ":"
                + byteToHexString(globalTransactionId)
                + ":"
                + byteToHexString(branchQualifier);
    }

    /**
     * Given an array of bytes it will convert the bytes to a hex string representation of the
     * bytes.
     *
     * @param bytes the bytes to convert in a hex string
     * @param start start index, inclusively
     * @param end end index, exclusively
     * @return hex string representation of the byte array
     */
    public static String byteToHexString(final byte[] bytes, final int start, final int end) {
        final int number0xf0 = 0xF0;
        final int number0x0f = 0x0F;
        final int number4 = 4;
        if (bytes == null) {
            throw new JdbcConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT, "bytes == null");
        }

        int length = end - start;
        char[] out = new char[length * 2];

        for (int i = start, j = 0; i < end; i++) {
            out[j++] = HEX_CHARS[(number0xf0 & bytes[i]) >>> number4];
            out[j++] = HEX_CHARS[number0x0f & bytes[i]];
        }

        return new String(out);
    }

    /**
     * Given an array of bytes it will convert the bytes to a hex string representation of the
     * bytes.
     *
     * @param bytes the bytes to convert in a hex string
     * @return hex string representation of the byte array
     */
    public static String byteToHexString(final byte[] bytes) {
        return byteToHexString(bytes, 0, bytes.length);
    }
}
