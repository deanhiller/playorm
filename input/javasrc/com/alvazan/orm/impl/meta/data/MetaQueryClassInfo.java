package com.alvazan.orm.impl.meta.data;

import java.util.Collection;

public interface MetaQueryClassInfo {

	Collection<? extends MetaQueryFieldInfo> getMetaFields();

	MetaQueryFieldInfo getMetaField(String attributeName);

	String getIdFieldName();

	@SuppressWarnings("rawtypes")
	Class getMetaClass();

}
