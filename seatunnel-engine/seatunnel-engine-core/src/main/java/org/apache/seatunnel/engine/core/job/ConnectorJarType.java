package org.apache.seatunnel.engine.core.job;

/** Connector jar package type, i.e. COMMON_PLUGIN_JAR or CONNECTOR_PLUGIN_JAR. */
public enum ConnectorJarType {
    /** Indicates a third-party Jar package that the corresponding connector plugin depends on. */
    COMMON_PLUGIN_JAR,
    /** Indicates a connector Jar package. */
    CONNECTOR_PLUGIN_JAR;
}
