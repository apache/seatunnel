package org.apache.seatunnel.admin.dto;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(value = "AlertMessagePage")
public class AlertMessagePage extends BasePage {

    @ApiModelProperty(name = "name", value = "ALERT_MESSAGE_FIELD_NAME")
    private String name;

    @ApiModelProperty(name = "status", value = "ALERT_MESSAGE_FIELD_STATUS", example = "1")
    private Integer status;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }
}
