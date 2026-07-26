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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.protocol;

public enum EdgeSocketResponseCode {

    /**
     * Request processed successfully. Also used as checkpoint watermark prefix via {@link
     * #withPayload}.
     */
    ACK("ACK"),

    /** Batch data received and enqueued for processing. */
    RECEIVED("RECEIVED"),

    /** Internal record queue is full; collector should back off and retry later. */
    QUEUE_FULL("QUEUE_FULL"),

    /**
     * Internal record queue is physically full; collector should apply exponential back-off and
     * retry.
     */
    RETRY("RETRY"),

    /** Commit query accepted but the batch has not yet been checkpointed; poll again later. */
    PENDING("PENDING"),

    /**
     * Returned by {@code __COMMIT__} when the source has no record of the queried batchId in the
     * current session (e.g. after a worker restart the collector reconnects with a batchId that was
     * never received in this execution attempt). The collector must resend the batch data via
     * {@code __BATCH__} before polling {@code __COMMIT__} again.
     */
    RESEND("RESEND"),

    /** Payload decode failed (e.g. decompression error, malformed JSON in PACKET mode). */
    DECODE_FAILED("DECODE_FAILED"),

    /** Payload decryption failed; typically indicates a token / key mismatch. */
    DECRYPT_FAILED("DECRYPT_FAILED"),

    /** Authentication token is missing or invalid. */
    AUTH_FAILED("AUTH_FAILED"),

    /** Request format is invalid (unrecognized command, missing separator, etc.). */
    BAD_REQUEST("BAD_REQUEST"),

    /** A required parameter value is invalid (e.g. non-numeric or non-positive batchId). */
    INVALID_PARAM("INVALID_PARAM"),

    /** Connection rejected (e.g. another collector is already connected). */
    REJECTED("REJECTED");

    private final String code;

    EdgeSocketResponseCode(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    /** Returns {@code "ACK:<payload>"}, used for checkpoint watermark responses. */
    public String withPayload(Object payload) {
        return code + ":" + payload;
    }

    @Override
    public String toString() {
        return code;
    }
}
