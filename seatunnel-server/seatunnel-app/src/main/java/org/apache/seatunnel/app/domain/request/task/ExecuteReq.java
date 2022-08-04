package org.apache.seatunnel.app.domain.request.task;

import lombok.Data;

import java.util.Date;
import java.util.Map;

@Data
public class ExecuteReq {
    private Integer scriptId;
    private String content;
    private Integer operatorId;
    private Map<String, Object> params;
    private int executeType;
    private Date startTime = new Date();
    private Date endTime = new Date();
    private Integer parallelismNum = 1;
}
