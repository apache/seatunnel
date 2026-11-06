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

package org.apache.seatunnel.resource.kubernetes.exception;

import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;

/**
 * Failure reported by the Kubernetes API or by the connection to it.
 *
 * <p>The provider reports its own exception instead of the client library's type, so callers and
 * the shaded artifact they compile against never depend on a relocated SDK package. The original
 * SDK failure stays available as the cause.
 */
public class KubernetesClientException extends SeaTunnelEngineException {

    private static final long serialVersionUID = 1L;

    /** HTTP status code of the failed request, or {@code 0} when it never reached the server. */
    private final int code;

    public KubernetesClientException(int code, String message) {
        super(message);
        this.code = code;
    }

    /**
     * @param code HTTP status code of the failed request, or {@code 0} for transport failures
     * @param message human readable failure description
     * @param cause original failure
     */
    public KubernetesClientException(int code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    /** @return HTTP status code, or {@code 0} when the request never reached the API server */
    public int getCode() {
        return code;
    }
}
