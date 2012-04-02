package com.alvazan.orm.impl.meta;

public class MetaClass<T> {

	private Class<T> metaClass;

	public void setMetaClass(Class<T> clazz) {
		this.metaClass = clazz;
	}

	public Class<T> getMetaClass() {
		return metaClass;
	}

	
}
