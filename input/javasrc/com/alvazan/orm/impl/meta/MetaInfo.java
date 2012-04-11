package com.alvazan.orm.impl.meta;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Singleton;

import com.alvazan.orm.layer2.nosql.Row;

@Singleton
public class MetaInfo {
	
	@SuppressWarnings("rawtypes")
	private Map<Class, MetaClass> classToClassMeta = new HashMap<Class, MetaClass>();
	
	@SuppressWarnings("rawtypes")
	public void addMeta(Class clazz, MetaClass classMeta) {
		classToClassMeta.put(clazz, classMeta);
	}

	@SuppressWarnings("rawtypes")
	public MetaClass getMetaClass(Object entity) {
		Class clazz = entity.getClass();
		MetaClass metaClass = classToClassMeta.get(clazz);
		return metaClass;
	}

}
