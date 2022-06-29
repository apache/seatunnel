package org.apache.seatunnel.admin.dto;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;

@ApiModel(value = "UserParam")
public class UserParam implements Serializable {

    @ApiModelProperty(name = "id", value = "USER_FIELD_ID")
    private Integer id;

    @ApiModelProperty(name = "username", value = "USER_FIELD_NAME")
    private String username;

    @ApiModelProperty(name = "password", value = "USER_FIELD_PASSWORD")
    private String password;

    @ApiModelProperty(name = "type", value = "USER_FIELD_TYPE")
    private Integer type;

    @ApiModelProperty(name = "email", value = "USER_FIELD_EMAIL", example = "100@163.com")
    private String email;

    @ApiModelProperty(name = "status", value = "USER_FIELD_STATUS", example = "1")
    private Integer status;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }
}
