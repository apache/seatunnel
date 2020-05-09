package io.github.interestinglab.waterdrop.config;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Allows an config property to be {@code null}.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Optional {

}
