package org.apache.seatunnel.connectors.seatunnel.redshift.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3Config;

public class S3RedshiftConfig extends S3Config {

    public static final Option<String> JDBC_URL = Options.key("jdbc_url").stringType().noDefaultValue().withDescription("Redshift JDBC URL");

    public static final Option<String> JDBC_USER = Options.key("jdbc_user").stringType().noDefaultValue().withDescription("Redshift JDBC user");

    public static final Option<String> JDBC_PASSWORD = Options.key("jdbc_password").stringType().noDefaultValue().withDescription("Redshift JDBC password");

    public static final Option<String> EXECUTE_SQL = Options.key("execute_sql").stringType().noDefaultValue().withDescription("Redshift execute sql");

}
