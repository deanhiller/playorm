package com.alvazan.orm.impl.meta.data;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

@SuppressWarnings("rawtypes")
@Singleton
public class MetaInfo {
	@Inject
	private Provider<MetaClass> classMetaProvider;
	
	private Map<Class, MetaClass> classToClassMeta = new HashMap<Class, MetaClass>();
	private Map<String, MetaClass> tableNameToClassMeta = new HashMap<String, MetaClass>();
	
	public MetaClass getMetaClass(Class clazz2) {
		Class clazz = clazz2; 
		if(NoSqlProxy.class.isAssignableFrom(clazz2)) {
			clazz = clazz2.getSuperclass();
		}
		MetaClass metaClass = classToClassMeta.get(clazz);
		return metaClass;
	}
	public MetaClass getMetaClass(String tableName) {
		return tableNameToClassMeta.get(tableName);
	}

	public MetaClass<?> findOrCreate(Class<?> clazz) {
		MetaClass<?> metaClass = classToClassMeta.get(clazz);
		if(metaClass != null)
			return metaClass;
		
		MetaClass<?> metaClass2 = classMetaProvider.get();
		classToClassMeta.put(clazz, metaClass2);
		
		return metaClass2;
	}

	
	public Collection<MetaClass> getAllEntities() {
		return classToClassMeta.values();
	}

	public void addTableNameLookup(MetaClass classMeta) {
		tableNameToClassMeta.put(classMeta.getColumnFamily(), classMeta);
	}
	
}
