package org.apache.seatunnel.connectors.seatunnel.neo4j.config;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;

@Getter
@Setter
public class Neo4jConfig implements Serializable {

    public static final String KEY_SINK_PLUGIN_NAME = "Neo4jSink";
    public static final String KEY_NEO4J_URI = "uri";
    public static final String KEY_USERNAME = "username";
    public static final String KEY_PASSWORD = "password";
    public static final String KEY_BEARER_TOKEN = "bearer_token";
    public static final String KEY_KERBEROS_TICKET = "kerberos_ticket"; // Base64 encoded

    public static final String KEY_DATABASE = "database";
    public static final String KEY_QUERY = "query";
    public static final String KEY_QUERY_PARAM_POSITION = "queryParamPosition";

    private DriverBuilder driverBuilder;
    private String query;
    private Map<String, Object> queryParamPosition;

}
