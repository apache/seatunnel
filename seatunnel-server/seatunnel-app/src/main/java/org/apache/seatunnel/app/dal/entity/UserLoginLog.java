package org.apache.seatunnel.app.dal.entity;

import lombok.Data;

import java.util.Date;

@Data
public class UserLoginLog {
    private Long id;

    private Integer userId;

    private String token;

    private Boolean tokenStatus;

    private Date createTime;

    private Date updateTime;
}
