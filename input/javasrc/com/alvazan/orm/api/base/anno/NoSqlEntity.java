package com.alvazan.orm.api.base.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface NoSqlEntity {
	/**
	 * This is the NAME of the column family OR the name of the virtual column family IF you use @NoSqlVirtualCf annotation
	 * Virtual Cf's are for when you play to have 1000's of Cf's as EVERY node in cassandra uses more memory every time you
	 * add a CF :(.  If you want nearly infinite CF's, you need to use virtual CF's which is what we did for a project.
	 * @return
	 */
    String columnfamily() default "";
}
