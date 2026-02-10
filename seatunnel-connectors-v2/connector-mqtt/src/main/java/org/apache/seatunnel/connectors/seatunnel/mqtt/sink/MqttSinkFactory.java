package org.apache.seatunnel.connectors.seatunnel.mqtt.sink;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;

public class MqttSinkFactory implements Factory {

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MQTT broker URL, e.g. tcp://localhost:1883");

    public static final Option<String> TOPIC =
            Options.key("topic").stringType().noDefaultValue().withDescription("Target MQTT topic");

    public static final Option<String> USERNAME =
            Options.key("username").stringType().noDefaultValue().withDescription("MQTT username");

    public static final Option<String> PASSWORD =
            Options.key("password").stringType().noDefaultValue().withDescription("MQTT password");

    public static final Option<Integer> QOS =
            Options.key("qos").intType().defaultValue(0).withDescription("MQTT QoS level (0 or 1)");

    public static final Option<String> FORMAT =
            Options.key("format")
                    .stringType()
                    .defaultValue("json")
                    .withDescription("Message serialization format (json or text)");

    @Override
    public String factoryIdentifier() {
        return "mqtt";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL, TOPIC)
                .optional(USERNAME, PASSWORD, QOS, FORMAT)
                .build();
    }
}
