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

package org.apache.seatunnel.server.common;

public enum SeatunnelErrorEnum {

    SCRIPT_ALREADY_EXIST(10001, "script already exist", "You already have a script with the same name : '%s'"),
    NO_SUCH_SCRIPT(10002, "no such script", "No such script. Maybe deleted by others."),
    USER_ALREADY_EXISTS(10003, "user already exist", "The same username [%s] is exist."),
    NO_SUCH_USER(10004, "no such user", "No such user. Maybe deleted by others."),
    SCHEDULER_CONFIG_NOT_EXIST(10005, "scheduler config not exist", "This script's scheduler config not exist, please check your config."),
    JSON_TRANSFORM_FAILED(10006, "json transform failed", "Json transform failed, it may be a bug."),

    USERNAME_PASSWORD_NO_MATCHED(10007, "username and password no matched", "The user name and password do not match, please check your input"),

    TOKEN_ILLEGAL(10008, "token illegal", "The token is expired or invalid, please login again."),

    /**
     * request dolphinscheduler failed
     */
    UNEXPECTED_RETURN_CODE(20000, "Unexpected return code", "Unexpected return code : [%s], error msg is [%s]"),
    QUERY_PROJECT_CODE_FAILED(20001, "query project code failed", "Request ds for querying project code failed"),
    NO_MATCHED_PROJECT(20002, "no matched project", "No matched project [%s], please check your configuration"),
    NO_MATCHED_SCRIPT_SAVE_DIR(20003, "no matched script save dir", "No matched script save dir [%s], please check your configuration"),
    GET_INSTANCE_FAILED(20004, "get instance failed", "Get instance failed"),

    NO_SUCH_ELEMENT(99995, "no such element", "No such element."),
    UNSUPPORTED_OPERATION(99996, "unsupported operation", "This operation [%s] is not supported now."),
    HTTP_REQUEST_FAILED(99997, "http request failed", "Http request failed, url is %s"),
    ILLEGAL_STATE(99998, "illegal state", "%s"),
    UNKNOWN(99999, "unknown exception", "Unknown exception")
    ;

    private final int code;
    private final String msg;
    private final String template;

    SeatunnelErrorEnum(int code, String msg, String template) {
        this.code = code;
        this.msg = msg;
        this.template = template;
    }

    public int getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }

    public String getTemplate() {
        return template;
    }
}
