/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.ConfigIncluder;
import io.github.interestinglab.waterdrop.config.ConfigIncluderClasspath;
import io.github.interestinglab.waterdrop.config.ConfigIncluderFile;
import io.github.interestinglab.waterdrop.config.ConfigIncluderURL;

interface FullIncluder extends ConfigIncluder, ConfigIncluderFile, ConfigIncluderURL,
            ConfigIncluderClasspath {

}
