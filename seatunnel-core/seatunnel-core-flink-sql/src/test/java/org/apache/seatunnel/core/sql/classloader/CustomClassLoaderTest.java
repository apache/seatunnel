package org.apache.seatunnel.core.sql.classloader;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class CustomClassLoaderTest {

    @Test
    public void testAddJar() {
        CustomClassLoader classLoader = new CustomClassLoader();

        File file = new File(ClassLoader.getSystemResource("flink.sql.conf.template").getPath());
        classLoader.addJar(file.toPath());
        Assert.assertEquals(classLoader.getURLs()[0].toString(), file.toURI().toString());
    }
}
