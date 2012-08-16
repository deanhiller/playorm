package com.alvazan.orm.impl.meta.data;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.alvazan.orm.api.base.anno.NoSqlInheritance;

@SuppressWarnings("rawtypes")
@Singleton
public class MetaInfo {
	@Inject
	private Provider<MetaClassSingle> classMetaProvider;
	@Inject
	private Provider<MetaClassInheritance> inheritanceProvider;
	
	private Map<Class, MetaAbstractClass> classToClassMeta = new HashMap<Class, MetaAbstractClass>();
	private Map<String, MetaAbstractClass> tableNameToClassMeta = new HashMap<String, MetaAbstractClass>();
	
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

	public void addSubclass(Class clazz, MetaClassInheritance parent) {
		classToClassMeta.put(clazz, parent);
	}
	
	@SuppressWarnings("unchecked")
	public MetaAbstractClass<?> findOrCreate(Class clazz) {
		MetaAbstractClass<?> metaClass = classToClassMeta.get(clazz);
		if(metaClass != null)
			return metaClass;

		Annotation annotation = clazz.getAnnotation(NoSqlInheritance.class);
		MetaAbstractClass<?> metaClass2;
		if(annotation != null)
			metaClass2 = inheritanceProvider.get();
		else
			metaClass2 = classMetaProvider.get();
		
		metaClass2.setMetaClass(clazz);
		classToClassMeta.put(clazz, metaClass2);
		
		return metaClass2;
	}

	
	public Collection<MetaAbstractClass> getAllEntities() {
		return classToClassMeta.values();
	}

	public void addTableNameLookup(MetaAbstractClass classMeta) {
		tableNameToClassMeta.put(classMeta.getColumnFamily(), classMeta);
	}
	public void clearAll() {
		classToClassMeta.clear();
		tableNameToClassMeta.clear();
	}
	
}
