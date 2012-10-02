package com.alvazan.orm.api.base.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface NoSqlVirtualCf {
	/**
	 * You can put a whole slew of tables in ONE ColumnFamily.  This maps which column family the entity
	 * will be stored in.
	 * @return
	 */
	String storedInCf();
	
}
