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

package org.apache.seatunnel.admin.enums;

import org.apache.seatunnel.admin.common.SpringContextHolder;

import org.springframework.context.MessageSource;
import org.springframework.context.i18n.LocaleContextHolder;

import java.util.Locale;

public enum ResultStatus {

    SUCCESS(0, "ENUM_RESULT_STATUS_0"),
    NOT_FOUND(404, "ENUM_RESULT_STATUS_404"),
    UNAUTHORIZED(403, "ENUM_RESULT_STATUS_403"),
    INTERNAL_SERVER_ERROR_ARGS(500, "ENUM_RESULT_STATUS_500"),
    REQUEST_PARAMS_NOT_VALID_ERROR(1001, "ENUM_RESULT_STATUS_1001"),
    USER_NAME_PASSWD_ERROR(1002, "ENUM_RESULT_STATUS_1002");

    private final int code;
    private final String messageCode;

    private ResultStatus(int code, String messageCode) {
        this.code = code;
        this.messageCode = messageCode;
    }

    public int getCode() {
        return this.code;
    }

    public String getMsg() {
        Locale locale = LocaleContextHolder.getLocale();
        MessageSource messageSource = SpringContextHolder.getBean(MessageSource.class);
        String message = messageSource.getMessage(messageCode, null, locale);
        return message;
    }
}
