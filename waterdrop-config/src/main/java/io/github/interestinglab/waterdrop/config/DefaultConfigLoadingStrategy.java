package io.github.interestinglab.waterdrop.config;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Default config loading strategy. Able to load resource, file or URL.
 * Behavior may be altered by defining one of VM properties
 * {@code config.resource}, {@code config.file} or {@code config.url}
 */
public class DefaultConfigLoadingStrategy implements ConfigLoadingStrategy {
    @Override
    public Config parseApplicationConfig(ConfigParseOptions parseOptions) {
        ClassLoader loader = parseOptions.getClassLoader();
        if (loader == null)
            throw new ConfigException.BugOrBroken(
                    "ClassLoader should have been set here; bug in ConfigFactory. "
                            + "(You can probably work around this bug by passing in a class loader or calling currentThread().setContextClassLoader() though.)");

        int specified = 0;

        // override application.conf with config.file, config.resource,
        // config.url if requested.
        String resource = System.getProperty("config.resource");
        if (resource != null)
            specified += 1;
        String file = System.getProperty("config.file");
        if (file != null)
            specified += 1;
        String url = System.getProperty("config.url");
        if (url != null)
            specified += 1;

        if (specified == 0) {
            return ConfigFactory.parseResourcesAnySyntax("application", parseOptions);
        } else if (specified > 1) {
            throw new ConfigException.Generic("You set more than one of config.file='" + file
                                                      + "', config.url='" + url + "', config.resource='" + resource
                                                      + "'; don't know which one to use!");
        } else {
            // the override file/url/resource MUST be present or it's an error
            ConfigParseOptions overrideOptions = parseOptions.setAllowMissing(false);
            if (resource != null) {
                if (resource.startsWith("/"))
                    resource = resource.substring(1);
                // this deliberately does not parseResourcesAnySyntax; if
                // people want that they can use an include statement.
                return ConfigFactory.parseResources(loader, resource, overrideOptions);
            } else if (file != null) {
                return ConfigFactory.parseFile(new File(file), overrideOptions);
            } else {
                try {
                    return ConfigFactory.parseURL(new URL(url), overrideOptions);
                } catch (MalformedURLException e) {
                    throw new ConfigException.Generic("Bad URL in config.url system property: '"
                                                              + url + "': " + e.getMessage(), e);
                }
            }
        }
    }
}
