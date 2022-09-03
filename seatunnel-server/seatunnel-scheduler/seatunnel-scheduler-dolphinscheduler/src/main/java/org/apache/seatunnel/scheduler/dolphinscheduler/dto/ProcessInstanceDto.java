package org.apache.seatunnel.scheduler.dolphinscheduler.dto;

import static org.apache.seatunnel.server.common.DateUtils.DEFAULT_DATETIME_FORMAT;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.util.Date;

@Data
public class ProcessInstanceDto {
    private int id;
    private long processDefinitionCode;
    private int processDefinitionVersion;
    private String state;
    private String recovery;
    @JsonFormat(pattern = DEFAULT_DATETIME_FORMAT)
    private Date startTime;
    @JsonFormat(pattern = DEFAULT_DATETIME_FORMAT)
    private Date endTime;
    private int runTimes;
    private String name;
    private String host;
    private String processDefinition;
    private String commandType;
    private String commandParam;
    private String taskDependType;
    private int maxTryTimes;
    private String failureStrategy;
    private String warningType;
    private String warningGroupId;
    @JsonFormat(pattern = DEFAULT_DATETIME_FORMAT)
    private Date scheduleTime;
    private String commandStartTime;
    private String globalParams;
    private String dagData;
    private int executorId;
    private String executorName;
    private String tenantCode;
    private String queue;
    private String isSubProcess;
    private String locations;
    private String historyCmd;
    private String dependenceScheduleTimes;
    private String duration;
    private String processInstancePriority;
    private String workerGroup;
    private String environmentCode;
    private int timeout;
    private int tenantId;
    private String varPool;
    private int nextProcessInstanceId;
    private int dryRun;
    @JsonFormat(pattern = DEFAULT_DATETIME_FORMAT)
    private Date restartTime;
    private String cmdTypeIfComplement;
    private boolean complementData;
    private boolean blocked;
    private boolean processInstanceStop;
}
