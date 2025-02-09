package org.apache.seatunnel.connectors.seatunnel.rabbitmq.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@AllArgsConstructor
public class RabbitmqSourceOptions extends RabbitmqOptions {
    public RabbitmqSourceOptions(Config config) {
        super(config);
    }
}
