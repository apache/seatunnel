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
