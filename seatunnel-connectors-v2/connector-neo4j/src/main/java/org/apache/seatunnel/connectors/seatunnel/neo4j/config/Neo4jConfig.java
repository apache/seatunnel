package org.apache.seatunnel.connectors.seatunnel.neo4j.config;

public class Neo4jConfig {

    public static final String SINK_PLUGIN_NAME = "Neo4jSink";
    public static final String NEO4J_URI = "uri";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String BEARER_TOKEN = "bearer_token";
    public static final String KERBEROS_TICKET = "kerberos_ticket"; // Base64 encoded

    private Neo4jConfig() {
    }
}
