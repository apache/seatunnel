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

package org.apache.seatunnel.transform.nlpmodel.llm;

import org.apache.seatunnel.transform.nlpmodel.ModelInvocationErrorType;
import org.apache.seatunnel.transform.nlpmodel.ModelInvocationException;

import java.net.SocketTimeoutException;
import java.util.regex.Pattern;

/** Builds bounded diagnostics without copying provider text or throwable chains. */
public final class LLMDiagnostics {
    private static final Pattern MODEL_IDENTIFIER =
            Pattern.compile("[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}");

    private LLMDiagnostics() {}

    public static ModelInvocationException sanitize(
            Exception failure, String provider, String model, ModelInvocationErrorType fallback) {
        return sanitize(failure, provider, model, fallback, null);
    }

    public static ModelInvocationException sanitize(
            Exception failure,
            String provider,
            String model,
            ModelInvocationErrorType fallback,
            Integer httpStatus) {
        ModelInvocationErrorType type = fallback;
        Integer status = httpStatus;
        boolean retryable = false;
        if (failure instanceof ModelInvocationException) {
            ModelInvocationException invocation = (ModelInvocationException) failure;
            type = invocation.getErrorType();
            status = invocation.getHttpStatus();
            retryable = invocation.isRetryable();
        } else if (failure instanceof SocketTimeoutException) {
            type = ModelInvocationErrorType.TIMEOUT;
        }
        // Even a typed failure may carry unsafe causes or suppressed cleanup exceptions.
        String safeModel =
                model != null && !model.contains("://") && MODEL_IDENTIFIER.matcher(model).matches()
                        ? model
                        : "[omitted]";
        return retryable
                ? ModelInvocationException.retryable(
                        type, provider, safeModel, "LLM request failed", status, null)
                : ModelInvocationException.nonRetryable(
                        type, provider, safeModel, "LLM request failed", status, null);
    }
}
