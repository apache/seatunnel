package org.apache.seatunnel.admin.dto;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;

@ApiModel(value = "UserPasswordParam")
public class UserPasswordParam implements Serializable {

    @ApiModelProperty(name = "id", value = "USER_FIELD_ID")
    private Integer id;

    @ApiModelProperty(name = "password", value = "USER_FIELD_PASSWORD")
    private String password;


    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

}
