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

package org.apache.seatunnel.transform.nlpmodel;

import java.io.IOException;

public class ModelInvocationException extends IOException {

    private static final int MAX_MESSAGE_LENGTH = 256;

    private final ModelInvocationErrorType errorType;
    private final boolean retryable;
    private final String provider;
    private final String model;
    private final Integer httpStatus;

    private ModelInvocationException(
            ModelInvocationErrorType errorType,
            boolean retryable,
            String provider,
            String model,
            String message,
            Integer httpStatus,
            Throwable cause) {
        super(buildMessage(errorType, retryable, provider, model, message, httpStatus), cause);
        this.errorType = errorType;
        this.retryable = retryable;
        this.provider = provider;
        this.model = model;
        this.httpStatus = httpStatus;
    }

    public static ModelInvocationException retryable(
            ModelInvocationErrorType errorType,
            String provider,
            String model,
            String message,
            Integer httpStatus,
            Throwable cause) {
        return new ModelInvocationException(
                errorType, true, provider, model, message, httpStatus, cause);
    }

    public static ModelInvocationException nonRetryable(
            ModelInvocationErrorType errorType,
            String provider,
            String model,
            String message,
            Integer httpStatus,
            Throwable cause) {
        return new ModelInvocationException(
                errorType, false, provider, model, message, httpStatus, cause);
    }

    public static ModelInvocationException nonRetryable(
            ModelInvocationErrorType errorType,
            String provider,
            String model,
            String message,
            Throwable cause) {
        return nonRetryable(errorType, provider, model, message, null, cause);
    }

    public static ModelInvocationException fromHttpStatus(
            String provider, String model, int statusCode, String ignoredResponseBody) {
        if (statusCode == 429) {
            return retryable(
                    ModelInvocationErrorType.RATE_LIMIT,
                    provider,
                    model,
                    "HTTP status " + statusCode,
                    statusCode,
                    null);
        }
        if (statusCode >= 500) {
            return retryable(
                    ModelInvocationErrorType.TEMPORARY_REMOTE_ERROR,
                    provider,
                    model,
                    "HTTP status " + statusCode,
                    statusCode,
                    null);
        }
        if (statusCode == 401 || statusCode == 403) {
            return nonRetryable(
                    ModelInvocationErrorType.AUTHENTICATION_ERROR,
                    provider,
                    model,
                    "HTTP status " + statusCode,
                    statusCode,
                    null);
        }
        if (statusCode >= 400) {
            return nonRetryable(
                    ModelInvocationErrorType.CONFIGURATION_ERROR,
                    provider,
                    model,
                    "HTTP status " + statusCode,
                    statusCode,
                    null);
        }
        return nonRetryable(
                ModelInvocationErrorType.UNKNOWN_REMOTE_ERROR,
                provider,
                model,
                "HTTP status " + statusCode,
                statusCode,
                null);
    }

    public ModelInvocationErrorType getErrorType() {
        return errorType;
    }

    public boolean isRetryable() {
        return retryable;
    }

    public String getProvider() {
        return provider;
    }

    public String getModel() {
        return model;
    }

    public Integer getHttpStatus() {
        return httpStatus;
    }

    private static String buildMessage(
            ModelInvocationErrorType errorType,
            boolean retryable,
            String provider,
            String model,
            String message,
            Integer httpStatus) {
        StringBuilder builder = new StringBuilder();
        builder.append("Model invocation failed");
        builder.append(": provider=").append(provider);
        builder.append(", model=").append(model);
        builder.append(", errorType=").append(errorType);
        builder.append(", retryable=").append(retryable);
        if (httpStatus != null) {
            builder.append(", httpStatus=").append(httpStatus);
        }
        String sanitized = sanitize(message);
        if (sanitized != null && !sanitized.isEmpty()) {
            builder.append(", message=").append(sanitized);
        }
        return builder.toString();
    }

    private static String sanitize(String message) {
        if (message == null) {
            return null;
        }
        String sanitized =
                message.replaceAll("(?i)(api[_-]?key\\s*[:=]\\s*)\\S+", "$1***")
                        .replaceAll("(?i)(secret[_-]?key\\s*[:=]\\s*)\\S+", "$1***")
                        .replaceAll("(?i)(authorization\\s*[:=]\\s*bearer\\s+)\\S+", "$1***");
        if (sanitized.length() > MAX_MESSAGE_LENGTH) {
            return sanitized.substring(0, MAX_MESSAGE_LENGTH) + "...";
        }
        return sanitized;
    }
}
