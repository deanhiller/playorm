package com.alvazan.orm.impl.meta;

import java.util.HashMap;
import java.util.Map;

public class MetaInfo {
	
	@SuppressWarnings("rawtypes")
	private Map<Class, MetaClass> classToClassMeta = new HashMap<Class, MetaClass>();
	
	@SuppressWarnings("rawtypes")
	public void addMeta(Class clazz, MetaClass classMeta) {
		classToClassMeta.put(clazz, classMeta);
	}

}
