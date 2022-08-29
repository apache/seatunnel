package org.apache.seatunnel.app.domain.request.task;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Builder;
import lombok.Data;


/**
 * @author 狄杰
 * @date 2022/8/27
 * @description
 */
@Data
@ApiModel(value = "instanceLogRes", description = "instance log")
@Builder
public class InstanceLogRes {
    @ApiModelProperty(value = "instance id", dataType = "long")
    private long instanceId;

    @ApiModelProperty(value = "instance id", dataType = "String")
    private String logContent;

    @ApiModelProperty(value = "skip line", dataType = "int")
    private int lastSkipLine;

    @ApiModelProperty(value = "limit number", dataType = "int")
    private int lastLimit;

    @ApiModelProperty(value = "is log end", dataType = "boolean")
    private boolean isEnd;
}
