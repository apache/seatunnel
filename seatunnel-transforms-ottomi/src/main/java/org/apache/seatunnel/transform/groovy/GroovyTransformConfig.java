package org.apache.seatunnel.transform.groovy;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class GroovyTransformConfig implements Serializable {

    private String code;

    public static final Option<String> CODE =
            Options.key("code").stringType().noDefaultValue().withDescription("Java Code");

    public static GroovyTransformConfig of(ReadonlyConfig config) {
        String code = config.getOptional(CODE).get();
        GroovyTransformConfig groovyTransformConfig = new GroovyTransformConfig();
        groovyTransformConfig.setCode(code);
        return groovyTransformConfig;
    }
}
