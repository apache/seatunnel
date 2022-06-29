package org.apache.seatunnel.admin.dto;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(value = "AlertConfigPage")
public class AlertConfigPage extends BasePage {

    @ApiModelProperty(name = "name", value = "ALERT_CONFIG_FIELD_NAME")
    private String name;

    @ApiModelProperty(name = "type", value = "ALERT_CONFIG_FIELD_TYPE")
    private String type;

    @ApiModelProperty(name = "configContent", value = "ALERT_CONFIG_FIELD_CONFIG_CONTENT")
    private String configContent;

    @ApiModelProperty(name = "status", value = "ALERT_CONFIG_FIELD_STATUS", example = "1")
    private Integer status;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getConfigContent() {
        return configContent;
    }

    public void setConfigContent(String configContent) {
        this.configContent = configContent;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

}
