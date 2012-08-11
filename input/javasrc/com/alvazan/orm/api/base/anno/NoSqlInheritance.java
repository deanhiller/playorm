package com.alvazan.orm.api.base.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface NoSqlInheritance {
    @SuppressWarnings("rawtypes")
	Class[] subclassesToScan();
    NoSqlInheritanceType strategy();
    String discriminatorColumnName() default "";
}
