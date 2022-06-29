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

import java.util.Locale;

import org.springframework.context.i18n.LocaleContextHolder;

public enum ResultStatus {

    SUCCESS(0, "success", "成功"),
    NOT_FOUND(404, "not found", "未找到"),
    UNAUTHORIZED(403, "unauthorized", "未授权"),
    INTERNAL_SERVER_ERROR_ARGS(500, "Internal Server Error: {0}", "服务端异常: {0}"),
    REQUEST_PARAMS_NOT_VALID_ERROR(1001, "request parameter {0} is not valid", "请求参数[{0}]无效"),
    USER_NAME_PASSWD_ERROR(1002,"user name or password error", "用户名或密码错误"),
    ;

    private final int code;
    private final String enMsg;
    private final String zhMsg;

    private ResultStatus(int code, String enMsg, String zhMsg) {
        this.code = code;
        this.enMsg = enMsg;
        this.zhMsg = zhMsg;
    }

    public int getCode() {
        return this.code;
    }

    public String getMsg() {
        if (Locale.SIMPLIFIED_CHINESE.getLanguage().equals(LocaleContextHolder.getLocale().getLanguage())) {
            return this.zhMsg;
        } else {
            return this.enMsg;
        }
    }
}
