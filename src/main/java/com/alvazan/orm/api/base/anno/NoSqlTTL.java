package com.alvazan.orm.api.base.anno;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Field contains TTL (Time to live) for entity
 * in seconds. Field must be int or Integer
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface NoSqlTTL {
    /* no arguments */
}