package com.alvazan.orm.impl.meta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
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
	
	private Map<String, List<MetaClass>> simpleNameToMatches = new HashMap<String, List<MetaClass>>();
	
	public MetaClass getMetaClass(Class clazz) {
		MetaClass metaClass = classToClassMeta.get(clazz);
		return metaClass;
	}

	MetaClass<?> findOrCreate(Class<?> clazz) {
		MetaClass<?> metaClass = classToClassMeta.get(clazz);
		if(metaClass != null)
			return metaClass;
		
		MetaClass<?> metaClass2 = classMetaProvider.get();
		classToClassMeta.put(clazz, metaClass2);
		
		//Also on create fill in the simpleNameToMatches as well
		addToSimpleNameToMatches(clazz, metaClass2);
		return metaClass2;
	}

	private void addToSimpleNameToMatches(Class<?> clazz,
			MetaClass<?> metaClass2) {
		String simpleName = clazz.getSimpleName();
		List<MetaClass> list = simpleNameToMatches.get(simpleName);
		if(list == null) {
			list = new ArrayList<MetaClass>();
			simpleNameToMatches.put(simpleName, list);
		}

		list.add(metaClass2);
	}

	public Collection<MetaClass> getAllEntities() {
		return classToClassMeta.values();
	}
	
	public List<MetaClass> findBySimpleName(String name) {
		return simpleNameToMatches.get(name);
	}
}
