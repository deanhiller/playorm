package com.alvazan.orm.api.base.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface OneToMany {
    @SuppressWarnings("rawtypes")
	Class entityType();

	String columnName() default "";

	/**
	 * When using Map instead of a List, the field in entityType() needs to 
	 * be specified here...
	 * @return
	 */
	String keyFieldForMap();

}
