package io.github.interestinglab.waterdrop.doctor;

import java.net.URL;

public class ClassLoaderUtils {

    public static URL getJarURL(Class cls) {
        return cls.getProtectionDomain().getCodeSource().getLocation();
    }

    public static URL getJarURL(String clsString) throws ClassNotFoundException {
        return getJarURL(Class.forName(clsString));
    }
}
