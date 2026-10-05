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

package org.apache.seatunnel.edge.agent.transport.socket;

public final class EdgeSocketProtocol {

    public static final String RESP_ACK = "ACK";
    public static final String RESP_RECEIVED = "RECEIVED";
    public static final String RESP_RETRY = "RETRY";
    public static final String RESP_AUTH_FAILED = "AUTH_FAILED";
    public static final String RESP_REJECTED = "REJECTED";
    public static final String RESP_DECRYPT_FAILED = "DECRYPT_FAILED";
    public static final String RESP_QUEUE_FULL_PREFIX = "QUEUE_FULL:";

    public static final String AUTH_LINE_PREFIX = "__AUTH__:";
    public static final String BATCH_PREFIX = "__BATCH__:";

    /** Default backoff when {@code QUEUE_FULL:<ms>} cannot be parsed. */
    public static final long DEFAULT_QUEUE_FULL_BACKOFF_MS = 500L;
}
