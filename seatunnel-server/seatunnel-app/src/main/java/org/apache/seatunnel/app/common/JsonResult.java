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

public class JsonResult<T> {

    private static final JsonResult<Void> OK = success();

    private int code = 0;

    private String msg;

    private T data;

    private JsonResult() {
        this.data = null;
    }

    private JsonResult(SeatunnelErrorEnum errorEnum) {
        this.code = errorEnum.getCode();
        this.msg = errorEnum.getMsg();
        this.data = null;
    }

    private JsonResult(SeatunnelErrorEnum errorEnum, String... messages) {
        this.code = errorEnum.getCode();
        this.msg = String.format(errorEnum.getTemplate(), messages);
        this.data = null;
    }

    public static <T> JsonResult<T> success() {
        return new JsonResult<>();
    }

    public static <T> JsonResult<T> success(T data) {
        JsonResult<T> result = success();
        result.setData(data);
        return result;
    }

    public static <T> JsonResult<T> failure(SeatunnelErrorEnum errorEnum) {
        JsonResult<T> result = new JsonResult<>(errorEnum);
        return result;
    }

    public static <T> JsonResult<T> failure(SeatunnelErrorEnum errorEnum, String... messages) {
        JsonResult<T> result = new JsonResult<>(errorEnum, messages);
        return result;
    }

    public boolean isSuccess() {
        return OK.getCode() == this.code;
    }

    public boolean isFailed() {
        return !this.isSuccess();
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
