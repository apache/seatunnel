/**
 *   Copyright (C) 2014 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.ConfigValue;

/**
 * An AbstractConfigValue which contains other values. Java has no way to
 * express "this has to be an AbstractConfigValue also" other than making
 * AbstractConfigValue an interface which would be aggravating. But we can say
 * we are a ConfigValue.
 */
interface Container extends ConfigValue {
    /**
     * Replace a child of this value. CAUTION if replacement is null, delete the
     * child, which may also delete the parent, or make the parent into a
     * non-container.
     */
    AbstractConfigValue replaceChild(AbstractConfigValue child, AbstractConfigValue replacement);

    /**
     * Super-expensive full traversal to see if descendant is anywhere
     * underneath this container.
     */
    boolean hasDescendant(AbstractConfigValue descendant);
}
