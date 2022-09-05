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

package org.apache.seatunnel.app.aspect;

import static org.apache.seatunnel.server.common.Constants.TOKEN;

import org.apache.seatunnel.app.common.Result;
import org.apache.seatunnel.app.common.UserTokenStatusEnum;
import org.apache.seatunnel.app.dal.dao.IUserDao;
import org.apache.seatunnel.app.domain.dto.user.UserLoginLogDto;
import org.apache.seatunnel.app.domain.response.user.UserSimpleInfoRes;
import org.apache.seatunnel.app.security.JwtUtils;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletResponse;

@Slf4j
@Aspect
@Component
@Order(2)
public class LoginAspect {
    @Resource
    private JwtUtils jwtUtils;

    @Resource
    private IUserDao userDaoImpl;

    @Pointcut("execution(public * org.apache.seatunnel.app.controller.UserController.login(..))")
    public void loginPointCut() {

    }

    @AfterReturning(value = "loginPointCut()", returning = "obj")
    public void check(JoinPoint pjp, Object obj) {
        final Result<UserSimpleInfoRes> target = (Result<UserSimpleInfoRes>) obj;
        final UserSimpleInfoRes data = target.getData();

        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        final HttpServletResponse response = attributes.getResponse();
        final String token = jwtUtils.genToken(data.toMap());
        response.setHeader(TOKEN, token);

        final UserLoginLogDto logDto = UserLoginLogDto.builder()
                .token(token)
                .tokenStatus(UserTokenStatusEnum.ENABLE.enable())
                .userId(data.getId())
                .build();
        userDaoImpl.insertLoginLog(logDto);
    }
}
