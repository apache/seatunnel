package org.apache.seatunnel.connectors.seatunnel.rabbitmq.config;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

@Setter
@Getter
@AllArgsConstructor
public class RabbitmqSinkOptions extends RabbitmqOptions {

    public RabbitmqSinkOptions(Config config){
        super(config);
    }
}
