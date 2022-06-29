package org.apache.seatunnel.admin.dto;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(value = "UserPage")
public class UserPage extends BasePage {

    @ApiModelProperty(name = "username", value = "USER_FIELD_NAME")
    private String username;

    @ApiModelProperty(name = "type", value = "USER_FIELD_TYPE")
    private String type;

    @ApiModelProperty(name = "status", value = "USER_FIELD_STATUS", example = "1")
    private Integer status;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }
}
