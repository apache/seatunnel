package com.blp.operator;

import lombok.Data;

import java.util.Map;

@Data
public class DataIngestionRequest {

    public String configJson;
    public String instanceName;
    public String jobId;
    public Map<String, String> files;
    // call this url when needed
    public String hookUrl;
}
