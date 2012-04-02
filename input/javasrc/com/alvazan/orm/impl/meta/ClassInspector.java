package com.alvazan.orm.impl.meta;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import com.alvazan.orm.api.anno.Embeddable;
import com.alvazan.orm.api.anno.NoSqlEntity;
import com.google.inject.Provider;

public class ClassInspector {

	@Inject
	private Provider<MetaClass<?>> classMetaProvider;
	@Inject
	private MetaInfo metaInfo;
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void addClass(Class clazz) {
		MetaClass<?> classMeta = classMetaProvider.get();
		classMeta.setMetaClass(clazz);
		scanClass(classMeta);
		metaInfo.addMeta(clazz, classMeta);
	}
	
	private void scanClass(MetaClass<?> meta) {
		NoSqlEntity noSqlEntity = meta.getMetaClass().getAnnotation(NoSqlEntity.class);
		Embeddable embeddable = meta.getMetaClass().getAnnotation(Embeddable.class);
		if(noSqlEntity != null) {
			String colFamily = noSqlEntity.columnfamily();
			if("".equals(colFamily))
				colFamily = meta.getMetaClass().getSimpleName()+"s";
			meta.setColumnFamily(colFamily);
		} else if(embeddable != null) {
			//nothing to do at this point
		} else {
			throw new RuntimeException("bug, someone added an annotation but didn't add an else if here");
		}
		
		scanFields(meta);
	}
	private void scanFields(MetaClass<?> meta) {
		Class<?> metaClass = meta.getMetaClass();
		List<Field[]> fields = new ArrayList<Field[]>();
		findFields(metaClass, fields);
		
		for(Field[] fieldArray : fields) {
			for(Field field : fieldArray) {
				inspectField(field);
			}
		}
	}

	private void inspectField(Field field) {
		if(Modifier.isTransient(field.getModifiers()) || 
				Modifier.isStatic(field.getModifiers()))
			return;
		
		
	}
	
	@SuppressWarnings("rawtypes")
	private void findFields(Class metaClass2, List<Field[]> fields) {
		Class next = metaClass2;
		while(true) {
			Field[] f = next.getDeclaredFields();
			fields.add(f);
			next = next.getSuperclass();
			if(next.equals(Object.class))
				return;
		}
	}

}
