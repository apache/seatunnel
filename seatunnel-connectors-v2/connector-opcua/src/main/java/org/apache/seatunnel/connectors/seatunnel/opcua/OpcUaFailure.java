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

package org.apache.seatunnel.connectors.seatunnel.opcua;

import java.io.IOException;

/** Sanitized connector failures never embed endpoint metadata, credentials, or values. */
final class OpcUaFailure extends IOException {
    private static final long serialVersionUID = 1L;

    final boolean retryable;
    final long statusCode;

    private OpcUaFailure(String text, boolean retryable, long statusCode) {
        super("OpcUa Source: " + text);
        this.retryable = retryable;
        this.statusCode = statusCode;
    }

    static OpcUaFailure problem(String text) {
        return new OpcUaFailure(text, false, -1L);
    }

    static OpcUaFailure serviceProblem(String text, boolean retryable, long statusCode) {
        return new OpcUaFailure(text, retryable, statusCode);
    }

    static OpcUaFailure transientProblem(String text) {
        return new OpcUaFailure(text, true, -1L);
    }
}
