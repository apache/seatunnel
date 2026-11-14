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

import org.apache.http.Header;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class ModelInvocationException extends IOException {

    private static final int MAX_MESSAGE_LENGTH = 256;

    private final ModelInvocationErrorType errorType;
    private final boolean retryable;
    private final String provider;
    private final String model;
    private final Integer httpStatus;
    private final Long retryAfterMs;

    private ModelInvocationException(
            ModelInvocationErrorType errorType,
            boolean retryable,
            String provider,
            String model,
            String message,
            Integer httpStatus,
            Throwable cause) {
        this(errorType, retryable, provider, model, message, httpStatus, cause, null);
    }

    private ModelInvocationException(
            ModelInvocationErrorType errorType,
            boolean retryable,
            String provider,
            String model,
            String message,
            Integer httpStatus,
            Throwable cause,
            Long retryAfterMs) {
        super(buildMessage(errorType, retryable, provider, model, message, httpStatus), cause);
        this.errorType = errorType;
        this.retryable = retryable;
        this.provider = provider;
        this.model = model;
        this.httpStatus = httpStatus;
        this.retryAfterMs = retryAfterMs;
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
        return fromHttpStatus(provider, model, statusCode, ignoredResponseBody, null);
    }

    public static ModelInvocationException fromHttpStatus(
            String provider,
            String model,
            int statusCode,
            String ignoredResponseBody,
            Header retryAfter) {
        ModelInvocationErrorType errorType;
        boolean retryable = false;
        if (statusCode == 429) {
            errorType = ModelInvocationErrorType.RATE_LIMIT;
            retryable = true;
        } else if (statusCode >= 500) {
            errorType = ModelInvocationErrorType.TEMPORARY_REMOTE_ERROR;
            retryable = true;
        } else if (statusCode == 401 || statusCode == 403) {
            errorType = ModelInvocationErrorType.AUTHENTICATION_ERROR;
        } else if (statusCode >= 400) {
            errorType = ModelInvocationErrorType.CONFIGURATION_ERROR;
        } else {
            errorType = ModelInvocationErrorType.UNKNOWN_REMOTE_ERROR;
        }
        return new ModelInvocationException(
                errorType,
                retryable,
                provider,
                model,
                "HTTP status " + statusCode,
                statusCode,
                null,
                retryable && retryAfter != null
                        ? parseRetryAfter(retryAfter.getValue(), System.currentTimeMillis())
                        : null);
    }

    static Long parseRetryAfter(String value, long nowMs) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        String trimmed = value.trim();
        boolean digits = true;
        for (int i = 0; i < trimmed.length(); i++) {
            char character = trimmed.charAt(i);
            if (character < '0' || character > '9') {
                digits = false;
                break;
            }
        }
        if (digits) {
            // Saturate valid but oversized delays so they cannot wrap into an early retry.
            long seconds = 0;
            for (int i = 0; i < trimmed.length(); i++) {
                int digit = trimmed.charAt(i) - '0';
                if (seconds > (Long.MAX_VALUE / 1000 - digit) / 10) {
                    return Long.MAX_VALUE;
                }
                seconds = seconds * 10 + digit;
            }
            return seconds * 1000;
        }
        for (String pattern :
                new String[] {
                    "EEE, dd MMM yyyy HH:mm:ss 'GMT'",
                    "EEEE, dd-MMM-yy HH:mm:ss 'GMT'",
                    "EEE MMM d HH:mm:ss yyyy"
                }) {
            SimpleDateFormat format = new SimpleDateFormat(pattern, Locale.US);
            format.setTimeZone(TimeZone.getTimeZone("GMT"));
            format.setLenient(false);
            Calendar centuryStart = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.US);
            centuryStart.setTimeInMillis(nowMs);
            centuryStart.add(Calendar.YEAR, -50);
            format.set2DigitYearStart(centuryStart.getTime());
            ParsePosition position = new ParsePosition(0);
            Date date = format.parse(trimmed, position);
            if (date != null && position.getIndex() == trimmed.length()) {
                return Math.max(0L, date.getTime() - nowMs);
            }
        }
        return null;
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

    public Long getRetryAfterMs() {
        return retryAfterMs;
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
