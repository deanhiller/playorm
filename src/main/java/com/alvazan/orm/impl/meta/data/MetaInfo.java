package com.alvazan.orm.impl.meta.data;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.anno.NoSqlInheritance;

@SuppressWarnings("rawtypes")
@Singleton
public class MetaInfo {
	private static final Logger log = LoggerFactory.getLogger(MetaInfo.class);
	
	@Inject
	private Provider<MetaClassSingle> classMetaProvider;
	@Inject
	private Provider<MetaClassInheritance> inheritanceProvider;
	
	private Map<String, MetaAbstractClass> classToClassMeta = new HashMap<String, MetaAbstractClass>();
	private Map<String, MetaAbstractClass> tableNameToClassMeta = new HashMap<String, MetaAbstractClass>();
	private Set<MetaClassInheritance> baseClasses = new HashSet<MetaClassInheritance>(); 
	
	public MetaClass getMetaClass(Class clazz2) {
		Class clazz = clazz2; 
		if(NoSqlProxy.class.isAssignableFrom(clazz2)) {
			clazz = clazz2.getSuperclass();
		}
		MetaClass metaClass = classToClassMeta.get(clazz.getName());
		return metaClass;
	}
	public MetaClass getMetaClass(String tableName) {
		return tableNameToClassMeta.get(tableName);
	}

	public void addSubclass(Class clazz, MetaClassInheritance parent) {
		classToClassMeta.put(clazz.getName(), parent);
		baseClasses.add(parent);
	}
	
	@SuppressWarnings("unchecked")
	public MetaAbstractClass<?> findOrCreate(Class clazz) {
		MetaAbstractClass<?> metaClass = classToClassMeta.get(clazz.getName());
		if(metaClass != null)
			return metaClass;

		Annotation annotation = clazz.getAnnotation(NoSqlInheritance.class);
		MetaAbstractClass<?> metaClass2;
		if(annotation != null)
			metaClass2 = inheritanceProvider.get();
		else
			metaClass2 = classMetaProvider.get();
		
		if(log.isDebugEnabled())
			log.debug("Adding mapping clazz="+clazz+" to type="+metaClass2.getClass().getSimpleName());
		
		metaClass2.setMetaClass(clazz);
		classToClassMeta.put(clazz.getName(), metaClass2);
		
		return metaClass2;
	}

	
	public Collection<MetaAbstractClass> getAllEntities() {
		//The random order every time we start is very annoying to me and to users. Let's order the values every
		//time instead
		List<MetaAbstractClass> all = new ArrayList<MetaAbstractClass>();
		for(MetaAbstractClass meta : classToClassMeta.values()) {
			all.add(meta);
		}
		Collections.sort(all, new MetaComparator());
		return all;
	}

	public void addTableNameLookup(MetaAbstractClass classMeta) {
		tableNameToClassMeta.put(classMeta.getColumnFamily(), classMeta);
	}
	public void clearAll() {
		classToClassMeta.clear();
		tableNameToClassMeta.clear();
	}
	public MetaClass lookupCf(String cf) {
		for(MetaClassInheritance p : baseClasses) {
			if(cf.equals(p.getMetaDbo().getRealColumnFamily()))
				return p;
		}
		
		for(MetaAbstractClass c : classToClassMeta.values()) {
			if(cf.equals(c.getMetaDbo().getRealColumnFamily()))
				return c;
		}

		return null;
	}
	
}
