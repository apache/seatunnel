package org.apache.seatunnel.app.domain.dto.user;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class UserLoginLogDto {
    private Long id;

    private Integer userId;

    private String token;

    private Boolean tokenStatus;
}
