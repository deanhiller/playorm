package com.alvazan.orm.impl.meta;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.inject.Provider;

@Singleton
public class MetaInfo {
	@Inject
	private Provider<MetaClass<?>> classMetaProvider;
	@SuppressWarnings("rawtypes")
	private Map<Class, MetaClass> classToClassMeta = new HashMap<Class, MetaClass>();
	
	@SuppressWarnings("rawtypes")
	public MetaClass getMetaClass(Object entity) {
		Class clazz = entity.getClass();
		MetaClass metaClass = classToClassMeta.get(clazz);
		return metaClass;
	}

	public MetaClass<?> findOrCreate(Class<?> clazz) {
		MetaClass<?> metaClass = classToClassMeta.get(clazz);
		if(metaClass != null)
			return metaClass;
		
		MetaClass<?> metaClass2 = classMetaProvider.get();
		classToClassMeta.put(clazz, metaClass2);
		return metaClass2;
	}
}
