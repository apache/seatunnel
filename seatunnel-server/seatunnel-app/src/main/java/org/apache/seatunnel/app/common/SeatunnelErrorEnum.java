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

package org.apache.seatunnel.app.common;

public enum SeatunnelErrorEnum {

    SCRIPT_ALREADY_EXIST(10001, "script already exist", "You already have a script with the same name : '%s'"),
    NO_SUCH_SCRIPT(10002, "no such script", "No such script. Maybe deleted by others."),
    USER_ALREADY_EXISTS(10003, "user already exist", "The same username [%s] is exist."),
    NO_SUCH_USER(10002, "no such user", "No such user. Maybe deleted by others."),
    ILLEGAL_STATE(99998, "illegal state", "%s"),
    UNKNOWN(99999, "unknown exception", "unknown exception")
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
