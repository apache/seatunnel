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

package org.apache.seatunnel.admin.exception;

import org.apache.seatunnel.admin.common.Result;
import org.apache.seatunnel.admin.enums.ResultStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.NoHandlerFoundException;

import javax.servlet.http.HttpServletRequest;

@RestControllerAdvice
@ResponseBody
public class GlobalExceptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(Exception.class)
    public Result<?> handleException(Exception e, HttpServletRequest request, HandlerMethod handlerMethod) {
        LOGGER.info("request url is " + request.getRequestURI());
        LOGGER.error(e.getMessage(), e);
        return Result.errorWithArgs(ResultStatus.INTERNAL_SERVER_ERROR_ARGS, e.getMessage());
    }

    @ExceptionHandler(SeatunnelServiceException.class)
    public Result<?> handleSeatunnelServiceException(SeatunnelServiceException e, HttpServletRequest request) {
        LOGGER.info("request url is " + request.getRequestURI());
        LOGGER.error(e.getMessage(), e);
        return Result.errorWithArgs(e.getResultStatus(), e.getMessage());
    }

    @ExceptionHandler(NoHandlerFoundException.class)
    public Result<?> handlerNoFoundException(Exception e) {
        LOGGER.error(e.getMessage(), e);
        return Result.errorWithArgs(ResultStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(value = MethodArgumentNotValidException.class)
    public Object handleMethodArgumentNotValidException(MethodArgumentNotValidException e, HttpServletRequest request) {
        LOGGER.info("request url is " + request.getRequestURI());
        LOGGER.error(e.getMessage(), e);
        BindingResult bindingResult = e.getBindingResult();
        String message = bindingResult.getAllErrors().get(0).getDefaultMessage();
        return Result.errorWithArgs(ResultStatus.INTERNAL_SERVER_ERROR_ARGS, message);
    }

}
