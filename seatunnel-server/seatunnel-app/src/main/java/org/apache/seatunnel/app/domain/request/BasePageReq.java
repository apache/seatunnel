package org.apache.seatunnel.app.domain.request;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
public class BasePageReq {
    @ApiModelProperty(value = "page number", required = true, dataType = "Integer")
    private Integer pageNo;
    @ApiModelProperty(value = "page size", required = true, dataType = "Integer")
    private Integer pageSize;

    public int getRealPageNo() {
        return pageNo - 1;
    }
}
