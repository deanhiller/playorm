package com.alvazan.orm.impl.meta;

public interface MetaQueryFieldInfo {

	@SuppressWarnings("rawtypes")
	Class getFieldType();

	String translateIfEntity(Object value);

}
