package com.alvazan.orm.impl.meta.data;

public interface MetaQueryFieldInfo {

	@SuppressWarnings("rawtypes")
	Class getFieldType();

	String translateIfEntity(Object value);

}
