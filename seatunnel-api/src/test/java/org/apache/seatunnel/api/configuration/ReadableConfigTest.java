package org.apache.seatunnel.api.configuration;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Properties;

public class ReadableConfigTest {
    private static final String CONFIG_PATH = "/conf/option-test.conf";

//    @BeforeAll
    @Test
    public void prepare() throws URISyntaxException, IOException {
        Config config = ConfigFactory
            .parseFile(Paths.get(this.getClass().getResource(CONFIG_PATH).toURI()).toFile())
            .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
            .resolveWith(ConfigFactory.systemProperties(),
                ConfigResolveOptions.defaults().setAllowUnresolved(true));


        String render = config.root().render(ConfigRenderOptions.concise());
        JavaPropsMapper mapper = new JavaPropsMapper();
        ObjectMapper om = new ObjectMapper();
        Object o = om.readValue(render, Object.class);
        Properties properties = mapper.writeValueAsProperties(o);
    }
}
