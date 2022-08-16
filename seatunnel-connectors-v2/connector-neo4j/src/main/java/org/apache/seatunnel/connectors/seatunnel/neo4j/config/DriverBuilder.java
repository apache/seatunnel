package org.apache.seatunnel.connectors.seatunnel.neo4j.config;

import lombok.Getter;
import lombok.Setter;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

import java.io.Serializable;
import java.net.URI;
import java.util.concurrent.TimeUnit;

@Getter
@Setter
public class DriverBuilder implements Serializable {
    private final URI uri;
    private String username;
    private String password;
    private String bearerToken;
    private String kerberosTicket;
    private String database;

    private Long maxTransactionRetryTimeSeconds;
    private Long maxConnectionTimeoutSeconds;

    public static DriverBuilder create(URI uri) {
        return new DriverBuilder(uri);
    }

    private DriverBuilder(URI uri) {
        this.uri = uri;
    }

    public Driver build() {
        final Config.ConfigBuilder configBuilder = Config.builder()
                .withMaxConnectionPoolSize(1);
        if (maxConnectionTimeoutSeconds != null) {
            configBuilder
                    .withConnectionAcquisitionTimeout(maxConnectionTimeoutSeconds * 2, TimeUnit.SECONDS)
                    .withConnectionTimeout(maxConnectionTimeoutSeconds, TimeUnit.SECONDS);
        }
        if (maxTransactionRetryTimeSeconds != null) {
            configBuilder
                    .withMaxTransactionRetryTime(maxTransactionRetryTimeSeconds, TimeUnit.SECONDS);
        }
        Config config = configBuilder
                .build();

        if (username != null) {
            return GraphDatabase.driver(uri, AuthTokens.basic(username, password), config);
        } else if (bearerToken != null) {
            return GraphDatabase.driver(uri, AuthTokens.bearer(bearerToken), config);
        } else if (kerberosTicket != null){
            return GraphDatabase.driver(uri, AuthTokens.kerberos(kerberosTicket), config);
        }
        throw new IllegalArgumentException("Invalid Field");
    }
}
