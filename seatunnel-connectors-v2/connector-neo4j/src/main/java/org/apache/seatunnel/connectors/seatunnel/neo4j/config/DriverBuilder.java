package org.apache.seatunnel.connectors.seatunnel.neo4j.config;

import lombok.Getter;
import lombok.Setter;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

import java.io.Serializable;
import java.net.URI;

@Getter
@Setter
public class DriverBuilder implements Serializable {
    private final URI uri;
    private String username;
    private String password;
    private String bearerToken;
    private String kerberosTicket;

    public static DriverBuilder create(URI uri) {
        return new DriverBuilder(uri);
    }

    private DriverBuilder(URI uri) {
        this.uri = uri;
    }

    public Driver build() {
        if (username != null) {
            return GraphDatabase.driver(uri, AuthTokens.basic(username, password));
        } else if (bearerToken != null) {
            return GraphDatabase.driver(uri, AuthTokens.bearer(bearerToken));
        } else if (kerberosTicket != null){
            return GraphDatabase.driver(uri, AuthTokens.kerberos(kerberosTicket));
        }
        throw new IllegalArgumentException("Invalid Field");
    }
}
