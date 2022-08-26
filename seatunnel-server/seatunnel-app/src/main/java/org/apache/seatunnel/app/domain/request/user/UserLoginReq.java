package org.apache.seatunnel.app.domain.request.user;

import lombok.Data;

@Data
public class UserLoginReq {
    private String username;
    private String password;
}
