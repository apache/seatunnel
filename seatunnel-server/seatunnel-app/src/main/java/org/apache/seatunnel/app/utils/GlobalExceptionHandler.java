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

package org.apache.seatunnel.app.utils;

import org.apache.seatunnel.app.common.Result;
import org.apache.seatunnel.server.common.SeatunnelErrorEnum;
import org.apache.seatunnel.server.common.SeatunnelException;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Optional;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(value = SeatunnelException.class)
    private Result<String> portalExceptionHandler(SeatunnelException e) {
        logError(e);

        final SeatunnelException seatunnelException = Optional.ofNullable(e).orElse(SeatunnelException.newInstance(SeatunnelErrorEnum.UNKNOWN));

        final String message = seatunnelException.getMessage();
        final SeatunnelErrorEnum errorEnum = seatunnelException.getErrorEnum();

        return Result.failure(errorEnum, message);
    }

    @ExceptionHandler(value = IllegalStateException.class)
    private Result<String> exceptionHandler(IllegalStateException e) {
        logError(e);
        return Result.failure(SeatunnelErrorEnum.ILLEGAL_STATE, e.getMessage());
    }

    @ExceptionHandler(value = Exception.class)
    private Result<String> exceptionHandler(Exception e) {
        logError(e);
        return Result.failure(SeatunnelErrorEnum.UNKNOWN, e.getMessage());
    }

    private void logError(Throwable throwable) {
        log.error(throwable.getMessage(), throwable);
    }

}
