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

package org.apache.seatunnel.transform.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.transform.common.ErrorHandleWay;

import lombok.Getter;

import java.util.Map;

public class ErrorDataTransformException extends SeaTunnelRuntimeException {
    @Getter private final ErrorHandleWay errorHandleWay;

    public ErrorDataTransformException(SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage) {
        this(null, seaTunnelErrorCode, errorMessage);
    }

    public ErrorDataTransformException(
            ErrorHandleWay errorHandleWay,
            SeaTunnelErrorCode seaTunnelErrorCode,
            String errorMessage) {
        super(seaTunnelErrorCode, errorMessage);
        this.errorHandleWay = errorHandleWay;
    }

    public ErrorDataTransformException(
            SeaTunnelErrorCode seaTunnelErrorCode, Map<String, String> params) {
        this(null, seaTunnelErrorCode, params);
    }

    public ErrorDataTransformException(
            ErrorHandleWay errorHandleWay,
            SeaTunnelErrorCode seaTunnelErrorCode,
            Map<String, String> params) {
        super(seaTunnelErrorCode, params);
        this.errorHandleWay = errorHandleWay;
    }
}
