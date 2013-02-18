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
	 * @return Name of the Virtual columnFamily in which this entity will be stored. 
	 */
	String storedInCf();
	
	//This will be the prefix of every key for this entity
	//String keyPrefix();
	
}
