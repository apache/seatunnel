package org.apache.seatunnel.admin.dto;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(value = "HostPage")
public class HostPage extends BasePage {

    @ApiModelProperty(name = "name", value = "HOST_FIELD_NAME")
    private String name;

    @ApiModelProperty(name = "host", value = "HOST_FIELD_TYPE")
    private String host;

    @ApiModelProperty(name = "status", value = "HOST_FIELD_STATUS", example = "1")
    private Integer status;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }
}
