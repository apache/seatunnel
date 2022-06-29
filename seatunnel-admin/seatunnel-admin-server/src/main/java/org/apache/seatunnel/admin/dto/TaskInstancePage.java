package org.apache.seatunnel.admin.dto;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(value = "TaskInstancePage")
public class TaskInstancePage extends BasePage {

    @ApiModelProperty(name = "name", value = "TASK_INSTANCE_FIELD_NAME")
    private String name;

    @ApiModelProperty(name = "type", value = "TASK_INSTANCE_FIELD_TYPE")
    private String type;

    @ApiModelProperty(name = "instanceStatus", value = "TASK_INSTANCE_FIELD_STATUS", example = "1")
    private Integer instanceStatus;

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

    public Integer getInstanceStatus() {
        return instanceStatus;
    }

    public void setInstanceStatus(Integer instanceStatus) {
        this.instanceStatus = instanceStatus;
    }
}
