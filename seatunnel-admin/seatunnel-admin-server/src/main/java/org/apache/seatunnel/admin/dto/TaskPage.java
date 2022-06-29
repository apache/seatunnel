package org.apache.seatunnel.admin.dto;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(value = "TaskPage")
public class TaskPage extends BasePage {

    @ApiModelProperty(name = "name", value = "TASK_FIELD_NAME")
    private String name;

    @ApiModelProperty(name = "type", value = "TASK_FIELD_TYPE")
    private String type;

    @ApiModelProperty(name = "tags", value = "TASK_FIELD_TAGS")
    private String tags;

    @ApiModelProperty(name = "status", value = "TASK_FIELD_STATUS", example = "1")
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

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }
}
